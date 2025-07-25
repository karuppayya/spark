/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.scripting

import java.util
import java.util.{Locale, UUID}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.{NameParameterizedQuery, UnresolvedAttribute, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateArray, CreateMap, CreateNamedStruct, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{CreateVariable, DefaultValueExpression, LogicalPlan, OneRowRelation, Project, SetVariable}
import org.apache.spark.sql.catalyst.plans.logical.ExceptionHandlerType.ExceptionHandlerType
import org.apache.spark.sql.catalyst.trees.{Origin, WithOrigin}
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.errors.SqlScriptingErrors
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * Trait for all SQL scripting execution nodes used during interpretation phase.
 */
sealed trait CompoundStatementExec extends Logging {

  /**
   * Whether the statement originates from the SQL script or is created during the interpretation.
   */
  val isInternal: Boolean = false

  /**
   * Reset execution of the current node.
   */
  def reset(): Unit
}

/**
 * Leaf node in the execution tree.
 */
trait LeafStatementExec extends CompoundStatementExec

/**
 * Non-leaf node in the execution tree. It is an iterator over executable child nodes.
 */
trait NonLeafStatementExec extends CompoundStatementExec {

  /** Pointer to the current statement - i.e. the statement that should be iterated next. */
  protected[scripting] var curr: Option[CompoundStatementExec]

  /**
   * Construct the iterator to traverse the tree rooted at this node in an in-order traversal.
   * @return
   *   Tree iterator.
   */
  def getTreeIterator: Iterator[CompoundStatementExec]

  /**
   * Evaluate the boolean condition represented by the statement.
   * @param session SparkSession that SQL script is executed within.
   * @param statement Statement representing the boolean condition to evaluate.
   * @return
   *    The value (`true` or `false`) of condition evaluation;
   *    or throw the error during the evaluation (eg: returning multiple rows of data
   *    or non-boolean statement).
   */
  protected def evaluateBooleanCondition(
      session: SparkSession,
      statement: LeafStatementExec): Boolean = statement match {
    case statement: SingleStatementExec =>
      assert(!statement.isExecuted)
      statement.isExecuted = true

      // First, it is checked if DataFrame represents a valid Boolean condition - single row,
      //   single column of Boolean type.
      // If that is true, the condition evaluates to True only if the Boolean value is True.
      //   Otherwise, if the Boolean value is False or NULL, the condition evaluates to False.
      val df = statement.buildDataFrame(session)
      df.schema.fields match {
        case Array(field) if field.dataType == BooleanType =>
          df.limit(2).collect() match {
            case Array(row) =>
              if (row.isNullAt(0)) false else row.getBoolean(0)
            case _ =>
              throw SparkException.internalError(
                s"Boolean statement ${statement.getText} is invalid. It returns more than one row.")
          }
        case _ =>
          throw SqlScriptingErrors.invalidBooleanStatement(statement.origin, statement.getText)
      }
    case _ =>
      throw SparkException.internalError("Boolean condition must be SingleStatementExec")
  }
}

/**
 * Executable node for SingleStatement.
 * @param parsedPlan
 *   Logical plan of the parsed statement.
 * @param origin
 *   Origin descriptor for the statement.
 * @param args
 *   A map of parameter names to SQL literal expressions.
 * @param isInternal
 *   Whether the statement originates from the SQL script or it is created during the
 *   interpretation.
 * @param context
 *   SqlScriptingExecutionContext keeps the execution state of current script.
 */
class SingleStatementExec(
    var parsedPlan: LogicalPlan,
    override val origin: Origin,
    val args: Map[String, Expression],
    override val isInternal: Boolean,
    context: SqlScriptingExecutionContext)
  extends LeafStatementExec with WithOrigin {

  /**
   * Whether this statement has been executed during the interpretation phase.
   * Example: Statements in conditions of If/Else, While, etc.
   */
  var isExecuted = false

  /**
   * Plan with named parameters.
   */
  private lazy val preparedPlan: LogicalPlan = {
    if (args.nonEmpty) {
      NameParameterizedQuery(parsedPlan, args)
    } else {
      parsedPlan
    }
  }

  /**
   * Get the SQL query text corresponding to this statement.
   * @return
   *   SQL query text.
   */
  def getText: String = {
    assert(origin.sqlText.isDefined && origin.startIndex.isDefined && origin.stopIndex.isDefined)
    origin.sqlText.get.substring(origin.startIndex.get, origin.stopIndex.get + 1)
  }

  /**
   * Builds a DataFrame from the parsedPlan of this SingleStatementExec
   * @param session The SparkSession on which the parsedPlan is built.
   * @return
   *   The DataFrame.
   */
  def buildDataFrame(session: SparkSession): DataFrame = {
    Dataset.ofRows(session, preparedPlan)
  }

  override def reset(): Unit = isExecuted = false
}

/**
 * NO-OP leaf node, which does nothing when returned to the iterator.
 * It is emitted by empty BEGIN END blocks.
 */
class NoOpStatementExec extends LeafStatementExec {
  override def reset(): Unit = ()
}

/**
 * Class to hold mapping of condition names/sqlStates to exception handlers
 * defined in a compound body.
 *
 * @param conditionToExceptionHandlerMap
 *   Map of condition names to exception handlers.
 * @param sqlStateToExceptionHandlerMap
 *   Map of sqlStates to exception handlers.
 * @param sqlExceptionHandler
 *   "Catch-all" exception handler.
 * @param notFoundHandler
 *   NOT FOUND exception handler.
 */
class TriggerToExceptionHandlerMap(
    conditionToExceptionHandlerMap: Map[String, ExceptionHandlerExec],
    sqlStateToExceptionHandlerMap: Map[String, ExceptionHandlerExec],
    sqlExceptionHandler: Option[ExceptionHandlerExec],
    notFoundHandler: Option[ExceptionHandlerExec]) {

  def getHandlerForCondition(condition: String): Option[ExceptionHandlerExec] = {
    conditionToExceptionHandlerMap.get(condition)
  }

  def getHandlerForSqlState(sqlState: String): Option[ExceptionHandlerExec] = {
    sqlStateToExceptionHandlerMap.get(sqlState)
  }

  def getSqlExceptionHandler: Option[ExceptionHandlerExec] = sqlExceptionHandler

  def getNotFoundHandler: Option[ExceptionHandlerExec] = notFoundHandler
}

object TriggerToExceptionHandlerMap {
  def createEmptyMap(): TriggerToExceptionHandlerMap = new TriggerToExceptionHandlerMap(
    Map.empty[String, ExceptionHandlerExec],
    Map.empty[String, ExceptionHandlerExec],
    None,
    None
  )
}

/**
 * Executable node for CompoundBody.
 * @param statements
 *   Executable nodes for nested statements within the CompoundBody.
 * @param label
 *   Label set by user to CompoundBody or None otherwise.
 * @param isScope
 *   Flag indicating if the CompoundBody is a labeled scope.
 *   Scopes are used for grouping local variables and exception handlers.
 * @param context
 *   SqlScriptingExecutionContext keeps the execution state of current script.
 * @param triggerToExceptionHandlerMap
 *   Map of condition names/sqlstates to error handlers defined in this compound body.
 */
class CompoundBodyExec(
    val statements: Seq[CompoundStatementExec],
    label: Option[String] = None,
    isScope: Boolean,
    context: SqlScriptingExecutionContext,
    triggerToExceptionHandlerMap: TriggerToExceptionHandlerMap)
  extends NonLeafStatementExec {

  private object ScopeStatus extends Enumeration {
    type ScopeStatus = Value
    val NOT_ENTERED, INSIDE, EXITED = Value
  }

  private var localIterator = statements.iterator
  protected[scripting] var curr: Option[CompoundStatementExec] =
    if (localIterator.hasNext) Some(localIterator.next()) else None
  private var scopeStatus = ScopeStatus.NOT_ENTERED

  /**
   * Enter scope represented by this compound statement.
   *
   * This operation needs to be idempotent because it is called multiple times during
   * iteration, but it should be executed only once when compound body that represent
   * scope is encountered for the first time.
   */
  private[scripting] def enterScope(): Unit = {
    // This check makes this operation idempotent.
    if (isScope && scopeStatus == ScopeStatus.NOT_ENTERED) {
      scopeStatus = ScopeStatus.INSIDE
      context.enterScope(label.get, triggerToExceptionHandlerMap)
    }
  }

  /**
   * Exit scope represented by this compound statement.
   *
   * Even though this operation is called exactly once, we are making it idempotent.
   */
  private[scripting] def exitScope(): Unit = {
    // This check makes this operation idempotent.
    if (isScope && scopeStatus == ScopeStatus.INSIDE) {
      scopeStatus = ScopeStatus.EXITED
      context.exitScope(label.get)
    }
  }

  /** Used to stop the iteration in cases when LEAVE statement is encountered. */
  private var stopIteration = false

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = {
        val childHasNext = curr match {
          case Some(body: NonLeafStatementExec) => body.getTreeIterator.hasNext
          case Some(_: LeafStatementExec) => true
          case None => false
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
        !stopIteration && (localIterator.hasNext || childHasNext)
      }

      @scala.annotation.tailrec
      override def next(): CompoundStatementExec = {
        curr match {
          case None => throw SparkException.internalError(
            "No more elements to iterate through in the current SQL compound statement.")
          case Some(leaveStatement: LeaveStatementExec) =>
            handleLeaveStatement(leaveStatement)
            curr = None
            leaveStatement
          case Some(iterateStatement: IterateStatementExec) =>
            handleIterateStatement(iterateStatement)
            curr = None
            iterateStatement
          case Some(statement: LeafStatementExec) =>
            curr = if (localIterator.hasNext) Some(localIterator.next()) else None
            statement
          case Some(body: NonLeafStatementExec) =>
            if (body.getTreeIterator.hasNext) {
              body match {
                // Scope will be entered only once per compound because enter scope is idempotent.
                case compoundBodyExec: CompoundBodyExec => compoundBodyExec.enterScope()
                case _ => // pass
              }
              body.getTreeIterator.next() match {
                case leaveStatement: LeaveStatementExec =>
                  handleLeaveStatement(leaveStatement)
                  leaveStatement
                case iterateStatement: IterateStatementExec =>
                  handleIterateStatement(iterateStatement)
                  iterateStatement
                case other => other
              }
            } else {
              body match {
                // Exit scope when there are no more statements to iterate through.
                case compoundBodyExec: CompoundBodyExec => compoundBodyExec.exitScope()
                case _ => // pass
              }
              curr = if (localIterator.hasNext) Some(localIterator.next()) else None
              next()
            }
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    statements.foreach(_.reset())
    localIterator = statements.iterator
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
    stopIteration = false
    scopeStatus = ScopeStatus.NOT_ENTERED
  }

  /** Actions to do when LEAVE statement is encountered, to stop the execution of this compound. */
  private def handleLeaveStatement(leaveStatement: LeaveStatementExec): Unit = {
    if (!leaveStatement.hasBeenMatched) {
      // Stop the iteration.
      stopIteration = true

      // Exit scope if leave statement is encountered.
      exitScope()

      // TODO: Variable cleanup (once we add SQL script execution logic).
      // TODO: Add interpreter tests as well.

      // Check if label has been matched.
      leaveStatement.hasBeenMatched = label.isDefined && label.get.equals(leaveStatement.label)
    }
  }

  /**
   * Actions to do when ITERATE statement is encountered, to stop the execution of this compound.
   */
  private def handleIterateStatement(iterateStatement: IterateStatementExec): Unit = {
    if (!iterateStatement.hasBeenMatched) {
      // Stop the iteration.
      stopIteration = true

      // Exit scope if iterate statement is encountered.
      exitScope()

      // TODO: Variable cleanup (once we add SQL script execution logic).
      // TODO: Add interpreter tests as well.

      // No need to check if label has been matched, since ITERATE statement is already
      //   not allowed in CompoundBody.
    }
  }
}

/**
 * Executable node for IfElseStatement.
 * @param conditions Collection of executable conditions. First condition corresponds to IF clause,
 *                   while others (if any) correspond to following ELSEIF clauses.
 * @param conditionalBodies Collection of executable bodies that have a corresponding condition,
*                 in IF or ELSEIF branches.
 * @param elseBody Body that is executed if none of the conditions are met,
 *                          i.e. ELSE branch.
 * @param session Spark session that SQL script is executed within.
 */
class IfElseStatementExec(
    conditions: Seq[SingleStatementExec],
    conditionalBodies: Seq[CompoundBodyExec],
    elseBody: Option[CompoundBodyExec],
    session: SparkSession) extends NonLeafStatementExec {
  private object IfElseState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = IfElseState.Condition
  protected[scripting] var curr: Option[CompoundStatementExec] = Some(conditions.head)

  private var clauseIdx: Int = 0
  private val conditionsCount = conditions.length
  assert(conditionsCount == conditionalBodies.length)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = {
        if (curr.exists(_.isInstanceOf[LeaveStatementExec])) {
          // Handling two cases when an exception is thrown:
          //   1. During condition evaluation - exception handling mechanism will replace condition
          //     with the appropriate LEAVE statement if the relevant condition handler was found.
          //   2. In the last statement of the body - curr would already be set to None when
          //     LEAVE statement is injected to it (i.e. LEAVE statement would replace None).
          return curr.get
        }

        state match {
          case IfElseState.Condition =>
            val condition = curr.get.asInstanceOf[SingleStatementExec]
            if (evaluateBooleanCondition(session, condition)) {
              state = IfElseState.Body
              curr = Some(conditionalBodies(clauseIdx))
            } else {
              clauseIdx += 1
              if (clauseIdx < conditionsCount) {
                // There are ELSEIF clauses remaining.
                state = IfElseState.Condition
                curr = Some(conditions(clauseIdx))
              } else if (elseBody.isDefined) {
                // ELSE clause exists.
                state = IfElseState.Body
                curr = Some(elseBody.get)
              } else {
                // No remaining clauses.
                curr = None
              }
            }
            condition
          case IfElseState.Body =>
            assert(curr.get.isInstanceOf[CompoundBodyExec])
            val currBody = curr.get.asInstanceOf[CompoundBodyExec]
            val retStmt = currBody.getTreeIterator.next()
            if (!currBody.getTreeIterator.hasNext) {
              curr = None
            }
            retStmt
        }
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = IfElseState.Condition
    curr = Some(conditions.head)
    clauseIdx = 0
    conditions.foreach(c => c.reset())
    conditionalBodies.foreach(b => b.reset())
    elseBody.foreach(b => b.reset())
  }
}

/**
 * Executable node for WhileStatement.
 * @param condition Executable node for the condition.
 * @param body Executable node for the body.
 * @param label Label set to WhileStatement by user or None otherwise.
 * @param session Spark session that SQL script is executed within.
 */
class WhileStatementExec(
    condition: SingleStatementExec,
    body: CompoundBodyExec,
    label: Option[String],
    session: SparkSession) extends NonLeafStatementExec {

  private object WhileState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = WhileState.Condition
  protected[scripting] var curr: Option[CompoundStatementExec] = Some(condition)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
          case WhileState.Condition =>
            curr match {
              case Some(leaveStatement: LeaveStatementExec) =>
                // Handling the case when condition evaluation throws an exception. Exception
                //   handling mechanism will replace condition with the appropriate LEAVE statement
                //   if the relevant condition handler was found.
                handleLeaveStatement(leaveStatement)
                leaveStatement
              case Some(condition: SingleStatementExec) =>
                if (evaluateBooleanCondition(session, condition)) {
                  state = WhileState.Body
                  curr = Some(body)
                  body.reset()
                } else {
                  curr = None
                }
                condition
              case _ =>
                throw SparkException.internalError("Unexpected statement type in WHILE condition.")
            }
          case WhileState.Body =>
            val retStmt = body.getTreeIterator.next()

            // Handle LEAVE or ITERATE statement if it has been encountered.
            retStmt match {
              case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
                handleLeaveStatement(leaveStatementExec)
                return retStmt
              case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
                if (label.contains(iterStatementExec.label)) {
                  iterStatementExec.hasBeenMatched = true
                }
                state = WhileState.Condition
                curr = Some(condition)
                condition.reset()
                return retStmt
              case _ =>
            }

            if (!body.getTreeIterator.hasNext) {
              state = WhileState.Condition
              curr = Some(condition)
              condition.reset()
            }
            retStmt
        }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = WhileState.Condition
    curr = Some(condition)
    condition.reset()
    body.reset()
  }

  private def handleLeaveStatement(leaveStatement: LeaveStatementExec): Unit = {
    if (label.contains(leaveStatement.label)) {
      leaveStatement.hasBeenMatched = true
    }
    curr = None
  }
}

/**
 * Executable node for SearchedCaseStatement.
 * @param conditions Collection of executable conditions which correspond to WHEN clauses.
 * @param conditionalBodies Collection of executable bodies that have a corresponding condition,
 *                 in WHEN branches.
 * @param elseBody Body that is executed if none of the conditions are met, i.e. ELSE branch.
 * @param session Spark session that SQL script is executed within.
 */
class SearchedCaseStatementExec(
    conditions: Seq[SingleStatementExec],
    conditionalBodies: Seq[CompoundBodyExec],
    elseBody: Option[CompoundBodyExec],
    session: SparkSession) extends NonLeafStatementExec {
  private object CaseState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = CaseState.Condition
  protected[scripting] var curr: Option[CompoundStatementExec] = Some(conditions.head)

  private var clauseIdx: Int = 0
  private val conditionsCount = conditions.length

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = {
        if (curr.exists(_.isInstanceOf[LeaveStatementExec])) {
          // Handling two cases when an exception is thrown:
          //   1. During condition evaluation - exception handling mechanism will replace condition
          //     with the appropriate LEAVE statement if the relevant condition handler was found.
          //   2. In the last statement of the body - curr would already be set to None when
          //     LEAVE statement is injected to it (i.e. LEAVE statement would replace None).
          return curr.get
        }

        state match {
          case CaseState.Condition =>
            val condition = curr.get.asInstanceOf[SingleStatementExec]
            if (evaluateBooleanCondition(session, condition)) {
              state = CaseState.Body
              curr = Some(conditionalBodies(clauseIdx))
            } else {
              clauseIdx += 1
              if (clauseIdx < conditionsCount) {
                // There are WHEN clauses remaining.
                state = CaseState.Condition
                curr = Some(conditions(clauseIdx))
              } else if (elseBody.isDefined) {
                // ELSE clause exists.
                state = CaseState.Body
                curr = Some(elseBody.get)
              } else {
                // No remaining clauses.
                curr = None
              }
            }
            condition
          case CaseState.Body =>
            assert(curr.get.isInstanceOf[CompoundBodyExec])
            val currBody = curr.get.asInstanceOf[CompoundBodyExec]
            val retStmt = currBody.getTreeIterator.next()
            if (!currBody.getTreeIterator.hasNext) {
              curr = None
            }
            retStmt
        }
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = CaseState.Condition
    curr = Some(conditions.head)
    clauseIdx = 0
    conditions.foreach(c => c.reset())
    conditionalBodies.foreach(b => b.reset())
    elseBody.foreach(b => b.reset())
  }
}

/**
 * Executable node for SimpleCaseStatement.
 * @param caseVariableExec Statement with which all conditionExpressions will be compared to.
 * @param conditionExpressions Collection of expressions which correspond to WHEN clauses.
 * @param conditionalBodies Collection of executable bodies that have a corresponding condition,
 *                 in WHEN branches.
 * @param elseBody Body that is executed if none of the conditions are met, i.e. ELSE branch.
 * @param session Spark session that SQL script is executed within.
 * @param context SqlScriptingExecutionContext keeps the execution state of current script.
 */
class SimpleCaseStatementExec(
    caseVariableExec: SingleStatementExec,
    conditionExpressions: Seq[Expression],
    conditionalBodies: Seq[CompoundBodyExec],
    elseBody: Option[CompoundBodyExec],
    session: SparkSession,
    context: SqlScriptingExecutionContext) extends NonLeafStatementExec {
  private object CaseState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = CaseState.Condition
  private var bodyExec: Option[CompoundBodyExec] = None

  protected[scripting] var curr: Option[CompoundStatementExec] = None

  private var conditionBodyTupleIterator: Iterator[(SingleStatementExec, CompoundBodyExec)] = _
  private var caseVariableLiteral: Literal = _

  private var isCacheValid = false
  private def validateCache(): Unit = {
    if (!isCacheValid) {
      val values = caseVariableExec.buildDataFrame(session).collect()
      caseVariableExec.isExecuted = true

      caseVariableLiteral = Literal(values.head.get(0))
      conditionBodyTupleIterator = createConditionBodyIterator
      isCacheValid = true
    }
  }

  private def cachedCaseVariableLiteral: Literal = {
    validateCache()
    caseVariableLiteral
  }

  private def cachedConditionBodyIterator: Iterator[(SingleStatementExec, CompoundBodyExec)] = {
    validateCache()
    conditionBodyTupleIterator
  }

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = state match {
        case CaseState.Condition =>
          // Equivalent to the "iteration hasn't started yet" - to avoid computing cache
          //   before the first actual iteration.
          curr.isEmpty ||
          // Special case when condition computation throws an exception.
          curr.exists(_.isInstanceOf[LeaveStatementExec]) ||
          // Regular conditions.
          cachedConditionBodyIterator.hasNext ||
          elseBody.isDefined
        case CaseState.Body => bodyExec.exists(_.getTreeIterator.hasNext)
      }

      override def next(): CompoundStatementExec = state match {
        case CaseState.Condition =>
          if (curr.exists(_.isInstanceOf[LeaveStatementExec])) {
            // Handling the case when condition evaluation throws an exception. Exception handling
            //   mechanism will replace condition with the appropriate LEAVE statement if the
            //   relevant condition handler was found.
            return curr.get
          }

          val nextOption = if (cachedConditionBodyIterator.hasNext) {
            Some(cachedConditionBodyIterator.next())
          } else {
            None
          }
          nextOption
            .map { case (condStmt, body) =>
              curr = Some(condStmt)
              if (evaluateBooleanCondition(session, condStmt)) {
                bodyExec = Some(body)
                curr = bodyExec
                state = CaseState.Body
              }
              condStmt
            }
            .orElse(elseBody.map { body => {
              bodyExec = Some(body)
              curr = bodyExec
              state = CaseState.Body
              next()
            }})
            .get
        case CaseState.Body => bodyExec.get.getTreeIterator.next()
      }
    }

  private def createConditionBodyIterator: Iterator[(SingleStatementExec, CompoundBodyExec)] =
    conditionExpressions.zip(conditionalBodies)
      .iterator
      .map { case (expr, body) =>
        val condition = Project(
          Seq(Alias(EqualTo(cachedCaseVariableLiteral, expr), "condition")()),
          OneRowRelation()
        )
        // We hack the Origin to provide more descriptive error messages. For example, if
        // the case variable is 1 and the condition expression it's compared to is 5, we
        // will get Origin with text "(1 = 5)".
        val conditionText = condition.projectList.head.asInstanceOf[Alias].child.toString
        val condStmt = new SingleStatementExec(
          condition,
          Origin(sqlText = Some(conditionText),
            startIndex = Some(0),
            stopIndex = Some(conditionText.length - 1),
            line = caseVariableExec.origin.line),
          Map.empty,
          isInternal = true,
          context = context
        )
        (condStmt, body)
      }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = CaseState.Condition
    bodyExec = None
    curr = None
    isCacheValid = false
    caseVariableExec.reset()
    conditionalBodies.foreach(b => b.reset())
    elseBody.foreach(b => b.reset())
  }
}

/**
 * Executable node for RepeatStatement.
 * @param condition Executable node for the condition - evaluates to a row with a single boolean
 *                  expression, otherwise throws an exception
 * @param body Executable node for the body.
 * @param label Label set to RepeatStatement by user, None if not set
 * @param session Spark session that SQL script is executed within.
 */
class RepeatStatementExec(
    condition: SingleStatementExec,
    body: CompoundBodyExec,
    label: Option[String],
    session: SparkSession) extends NonLeafStatementExec {

  private object RepeatState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = RepeatState.Body
  protected[scripting] var curr: Option[CompoundStatementExec] = Some(body)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
        case RepeatState.Condition =>
          curr match {
            case Some(leaveStatement: LeaveStatementExec) =>
              // Handling the case when condition evaluation throws an exception. Exception
              //   handling mechanism will replace condition with the appropriate LEAVE statement
              //   if the relevant condition handler was found.
              handleLeaveStatement(leaveStatement)
              leaveStatement
            case Some(condition: SingleStatementExec) =>
              if (!evaluateBooleanCondition(session, condition)) {
                state = RepeatState.Body
                curr = Some(body)
                body.reset()
              } else {
                curr = None
              }
              condition
            case _ =>
              throw SparkException.internalError("Unexpected statement type in REPEAT condition.")
          }
        case RepeatState.Body =>
          val retStmt = body.getTreeIterator.next()

          retStmt match {
            case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
              handleLeaveStatement(leaveStatementExec)
              return retStmt
            case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
              if (label.contains(iterStatementExec.label)) {
                iterStatementExec.hasBeenMatched = true
              }
              state = RepeatState.Condition
              curr = Some(condition)
              condition.reset()
              return retStmt
            case _ =>
          }

          if (!body.getTreeIterator.hasNext) {
            state = RepeatState.Condition
            curr = Some(condition)
            condition.reset()
          }
          retStmt
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = RepeatState.Body
    curr = Some(body)
    body.reset()
    condition.reset()
  }

  private def handleLeaveStatement(leaveStatement: LeaveStatementExec): Unit = {
    if (label.contains(leaveStatement.label)) {
      leaveStatement.hasBeenMatched = true
    }
    curr = None
  }
}

/**
 * Executable node for LeaveStatement.
 * @param label Label of the compound or loop to leave.
 */
class LeaveStatementExec(val label: String) extends LeafStatementExec {
  /**
   * Label specified in the LEAVE statement might not belong to the immediate surrounding compound,
   *   but to the any surrounding compound.
   * Iteration logic is recursive, i.e. when iterating through the compound, if another
   *   compound is encountered, next() will be called to iterate its body. The same logic
   *   is applied to any other compound down the traversal tree.
   * In such cases, when LEAVE statement is encountered (as the leaf of the traversal tree),
   *   it will be propagated upwards and the logic will try to match it to the labels of
   *   surrounding compounds.
   * Once the match is found, this flag is set to true to indicate that search should be stopped.
   */
  var hasBeenMatched: Boolean = false
  override def reset(): Unit = hasBeenMatched = false
}

/**
 * Executable node for ITERATE statement.
 * @param label Label of the loop to iterate.
 */
class IterateStatementExec(val label: String) extends LeafStatementExec {
  /**
   * Label specified in the ITERATE statement might not belong to the immediate compound,
   * but to the any surrounding compound.
   * Iteration logic is recursive, i.e. when iterating through the compound, if another
   * compound is encountered, next() will be called to iterate its body. The same logic
   * is applied to any other compound down the tree.
   * In such cases, when ITERATE statement is encountered (as the leaf of the traversal tree),
   * it will be propagated upwards and the logic will try to match it to the labels of
   * surrounding compounds.
   * Once the match is found, this flag is set to true to indicate that search should be stopped.
   */
  var hasBeenMatched: Boolean = false
  override def reset(): Unit = hasBeenMatched = false
}

/**
 * Executable node for LoopStatement.
 * @param body Executable node for the body, executed on every loop iteration.
 * @param label Label set to LoopStatement by user, None if not set.
 */
class LoopStatementExec(
    body: CompoundBodyExec,
    val label: Option[String]) extends NonLeafStatementExec {

  protected[scripting] var curr: Option[CompoundStatementExec] = Some(body)

  /**
   * Loop can be interrupted by LeaveStatementExec
   */
  private var interrupted: Boolean = false

  /**
   * Loop can be iterated by IterateStatementExec
   */
  private var iterated: Boolean = false

  private lazy val treeIterator =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = !interrupted

      override def next(): CompoundStatementExec = {
        if (!body.getTreeIterator.hasNext || iterated) {
          reset()
        }

        val retStmt = body.getTreeIterator.next()

        retStmt match {
          case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
            if (label.contains(leaveStatementExec.label)) {
              leaveStatementExec.hasBeenMatched = true
            }
            interrupted = true
          case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
            if (label.contains(iterStatementExec.label)) {
              iterStatementExec.hasBeenMatched = true
            }
            iterated = true
          case _ =>
        }

        retStmt
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    interrupted = false
    iterated = false
    body.reset()
  }
}

/**
 * Executable node for ForStatement.
 * @param query Executable node for the query.
 * @param variableName Name of variable used for accessing current row during iteration.
 * @param statements List of statements to be executed in the FOR body.
 * @param label Label set to ForStatement by user or None otherwise.
 * @param session Spark session that SQL script is executed within.
 * @param context SqlScriptingExecutionContext keeps the execution state of current script.
 */
class ForStatementExec(
    query: SingleStatementExec,
    variableName: Option[String],
    statements: Seq[CompoundStatementExec],
    val label: Option[String],
    session: SparkSession,
    context: SqlScriptingExecutionContext) extends NonLeafStatementExec {

  private object ForState extends Enumeration {
    val VariableAssignment, Body = Value
  }
  private var state = ForState.VariableAssignment

  private var queryResult: util.Iterator[Row] = _
  private var queryColumnNameToDataType: Map[String, DataType] = _
  private var isResultCacheValid = false
  private def cachedQueryResult(): util.Iterator[Row] = {
    if (!isResultCacheValid) {
      val df = query.buildDataFrame(session)
      queryResult = df.toLocalIterator()
      queryColumnNameToDataType = df.schema.fields.map(f => f.name -> f.dataType).toMap

      query.isExecuted = true
      isResultCacheValid = true
    }
    queryResult
  }

  protected[scripting] var curr: Option[CompoundStatementExec] = None

  private var bodyWithVariables: Option[CompoundBodyExec] = None

  /**
   * For can be interrupted by LeaveStatementExec
   */
  private var interrupted: Boolean = false

  /**
   * Whether this iteration of the FOR loop is the first one.
   */
  private var firstIteration: Boolean = true

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {

      override def hasNext: Boolean = !interrupted && (state match {
        // `firstIteration` NEEDS to be the first condition! This is to handle edge-cases when
        //   query fails with an exception. If the `cachedQueryResult().hasNext` is first, this
        //   would mean that exception would be thrown before the scope of the parent (which is
        //   of CompoundBodyExec type) of the FOR statement is entered (required for proper
        //   exception handling). This can happen in a case when FOR statement is a first
        //   statement in the compound.
        case ForState.VariableAssignment => firstIteration || cachedQueryResult().hasNext
        case ForState.Body => bodyWithVariables.exists(_.getTreeIterator.hasNext)
      })

      override def next(): CompoundStatementExec = state match {

        case ForState.VariableAssignment =>
          if (curr.exists(_.isInstanceOf[LeaveStatementExec])) {
            // Handling the case when condition evaluation throws an exception. Exception handling
            //   mechanism will replace condition with the appropriate LEAVE statement if the
            //   relevant condition handler was found.
            val leaveStatement = curr.get.asInstanceOf[LeaveStatementExec]
            handleLeaveStatement(leaveStatement)
            return leaveStatement
          }

          // If result set is empty, and we are on the first iteration, we return NO-OP statement
          // to prevent compound statements from not having anything to return. For example,
          // if a FOR statement is nested in REPEAT, REPEAT will assume that FOR has at least
          // one statement to return. In the case the result set is empty, FOR doesn't have
          // anything to return naturally, so we return NO-OP instead.
          if (!cachedQueryResult().hasNext && firstIteration) {
            firstIteration = false
            return new NoOpStatementExec
          }
          firstIteration = false

          val row = cachedQueryResult().next()

          val variableInitStatements = row.schema.names.toSeq
            .map { colName => (colName, createExpressionFromValue(row.getAs(colName))) }
            .flatMap { case (colName, expr) => Seq(
              createDeclareVarExec(colName),
              createSetVarExec(colName, expr)
            ) }

          val varScopeLabel = variableName.map(varName =>
            if (session.sessionState.conf.caseSensitiveAnalysis) {
              varName
            } else {
              varName.toLowerCase(Locale.ROOT)
            }
          ).orElse(Some(UUID.randomUUID().toString.toLowerCase(Locale.ROOT)))

          bodyWithVariables = Some(new CompoundBodyExec(
            // NoOpStatementExec appended to end of body to prevent
            // dropping variables before last statement is executed.
            // This is necessary because we are calling exitScope before returning the last
            // statement, so we need the last statement to be NoOp.
            statements = variableInitStatements ++ statements :+ new NoOpStatementExec,
            // We generate label name if FOR variable is not specified, similar to how
            // compound bodies have generated label names if label is not specified.
            label = varScopeLabel,
            isScope = true,
            context = context,
            triggerToExceptionHandlerMap = TriggerToExceptionHandlerMap.createEmptyMap()
          ))

          state = ForState.Body
          bodyWithVariables.foreach(_.reset())
          bodyWithVariables.foreach(_.enterScope())
          curr = bodyWithVariables
          next()

        case ForState.Body =>
          // `bodyWithVariables` must be defined at this point.
          assert(bodyWithVariables.isDefined)
          val retStmt = bodyWithVariables.get.getTreeIterator.next()

          // Handle LEAVE or ITERATE statement if it has been encountered.
          retStmt match {
            case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
              handleLeaveStatement(leaveStatementExec)
              return retStmt
            case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
              if (label.contains(iterStatementExec.label)) {
                iterStatementExec.hasBeenMatched = true
              } else {
                // If an outer loop is being iterated, we need to exit the scope, as
                // we will not reach the point where we usually exit it.
                bodyWithVariables.foreach(_.exitScope())
              }
              state = ForState.VariableAssignment
              return retStmt
            case _ =>
          }

          if (!bodyWithVariables.exists(_.getTreeIterator.hasNext)) {
            bodyWithVariables.foreach(_.exitScope())
            curr = None
            state = ForState.VariableAssignment
          }
          retStmt
      }
    }

    private def handleLeaveStatement(leaveStatement: LeaveStatementExec): Unit = {
      if (label.contains(leaveStatement.label)) {
        leaveStatement.hasBeenMatched = true
      }
      interrupted = true
      // If this for statement encounters LEAVE, we need to exit the scope, as
      // we will not reach the point where we usually exit it.
      bodyWithVariables.foreach(_.exitScope())
    }

  /**
   * Recursively creates a Catalyst expression from Scala value.<br>
   * See https://spark.apache.org/docs/latest/sql-ref-datatypes.html for Spark -> Scala mappings
   */
  private def createExpressionFromValue(value: Any): Expression = value match {
    case m: Map[_, _] =>
      // arguments of CreateMap are in the format: (key1, val1, key2, val2, ...)
      val mapArgs = m.keys.toSeq.flatMap { key =>
        Seq(createExpressionFromValue(key), createExpressionFromValue(m(key)))
      }
      CreateMap(mapArgs, useStringTypeWhenEmpty = false)

    // structs and rows match this case
    case s: Row =>
    // arguments of CreateNamedStruct are in the format: (name1, val1, name2, val2, ...)
    val namedStructArgs = s.schema.names.toSeq.flatMap { colName =>
        val valueExpression = createExpressionFromValue(s.getAs(colName))
        Seq(Literal(colName), valueExpression)
      }
      CreateNamedStruct(namedStructArgs)

    // arrays match this case
    case a: collection.Seq[_] =>
      val arrayArgs = a.toSeq.map(createExpressionFromValue(_))
      CreateArray(arrayArgs, useStringTypeWhenEmpty = false)

    case _ => Literal(value)
  }

  private def createDeclareVarExec(varName: String): SingleStatementExec = {
    val defaultExpression = DefaultValueExpression(
      Literal(null, queryColumnNameToDataType(varName)), "null")
    val declareVariable = CreateVariable(
      UnresolvedIdentifier(Seq(varName)),
      defaultExpression,
      replace = false
    )
    new SingleStatementExec(declareVariable, Origin(), Map.empty, isInternal = true, context)
  }

  private def createSetVarExec(varName: String, variable: Expression): SingleStatementExec = {
    val projectNamedStruct = Project(
      Seq(Alias(variable, varName)()),
      OneRowRelation()
    )
    val setIdentifierToCurrentRow =
      SetVariable(Seq(UnresolvedAttribute.quoted(varName)), projectNamedStruct)
    new SingleStatementExec(
      setIdentifierToCurrentRow,
      Origin(),
      Map.empty,
      isInternal = true,
      context)
  }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = ForState.VariableAssignment
    isResultCacheValid = false
    interrupted = false
    curr = None
    bodyWithVariables = None
    firstIteration = true
  }
}

/**
 * Executable node for ExceptionHandler.
 * @param body Executable CompoundBody of the exception handler.
 * @param handlerType Handler type: EXIT, CONTINUE.
 * @param scopeLabel Label of the scope where handler is defined.
 */
class ExceptionHandlerExec(
    val body: CompoundBodyExec,
    val handlerType: ExceptionHandlerType,
    val scopeLabel: Option[String]) extends NonLeafStatementExec {

  protected[scripting] var curr: Option[CompoundStatementExec] = body.curr

  override def getTreeIterator: Iterator[CompoundStatementExec] = body.getTreeIterator

  override def reset(): Unit = body.reset()
}
