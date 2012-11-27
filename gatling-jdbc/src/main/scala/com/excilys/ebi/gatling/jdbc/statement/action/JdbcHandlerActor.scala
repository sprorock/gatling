/**
 * Copyright 2011-2012 eBusiness Information, Groupe Excilys (www.excilys.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.excilys.ebi.gatling.jdbc.statement.action

import java.lang.System.nanoTime
import java.sql.{Connection, PreparedStatement, SQLException}

import scala.math.max

import com.excilys.ebi.gatling.core.action.BaseActor
import com.excilys.ebi.gatling.core.config.GatlingConfiguration.configuration
import com.excilys.ebi.gatling.core.result.message.{ KO, OK, RequestStatus }
import com.excilys.ebi.gatling.core.result.writer.DataWriter
import com.excilys.ebi.gatling.core.session.Session
import com.excilys.ebi.gatling.core.util.IOHelper.use
import com.excilys.ebi.gatling.core.util.TimeHelper.{ computeTimeMillisFromNanos,nowMillis }
import com.excilys.ebi.gatling.jdbc.statement.builder.AbstractJdbcStatementBuilder
import com.excilys.ebi.gatling.jdbc.util.RowIterator.ResultSet2RowIterator
import com.excilys.ebi.gatling.jdbc.util.JdbcHelper._

import akka.actor.{ ActorRef, ReceiveTimeout }
import akka.util.duration.intToDurationInt

// Message to start execution of query by the actor
object ExecuteStatement

object JdbcHandlerActor {

	def apply(statementName: String,builder: AbstractJdbcStatementBuilder[_],isolationLevel: Option[Int],session: Session,next: ActorRef) =
		new JdbcHandlerActor(statementName,builder,isolationLevel,session,next)
}

class JdbcHandlerActor(statementName: String,builder: AbstractJdbcStatementBuilder[_],isolationLevel: Option[Int],session: Session,next: ActorRef) extends BaseActor {

	var executionStartDate = nowMillis
	var statementExecutionStartDate = 0L
	var statementExecutionEndDate = 0L
	var executionEndDate = 0L

	def receive = {
		case ExecuteStatement => execute
		case ReceiveTimeout =>
			logStatement(KO,Some("JdbcHandlerActor timed out"))
			executeNext(session.setFailed)
	}

	private def execute = {
		var statement: PreparedStatement = null
		var connection: Connection = null
		try {
			// Fetch connection
			connection = getConnection(session)
			if(isolationLevel.isDefined) connection.setTransactionIsolation(isolationLevel.get)
			resetTimeout
			// Execute statement
			val (bindingOk,statement) = builder.bindParams(session,getStatement(session,connection,builder))
			// If parameter binding is OK
			if (bindingOk) {
				statementExecutionStartDate = computeTimeMillisFromNanos(nanoTime)
				val hasResultSet = statement.execute
				statementExecutionEndDate = computeTimeMillisFromNanos(nanoTime)
				resetTimeout
				// Process result set
				if (hasResultSet) processResultSet(statement)
				executionEndDate = computeTimeMillisFromNanos(nanoTime)
				logStatement(OK)
			}
			next ! session
			context.stop(self)

		} catch {
			case sqle : SQLException =>
				logStatement(KO,Some(sqle.getMessage))
				executeNext(session.setFailed)
		} finally {
			closeStatement(session,statement)
			closeConnection(session,connection)
		}
	}

	private def executeNext(newSession: Session) = next ! newSession.increaseTimeShift(nowMillis - executionEndDate)

	private def resetTimeout = context.setReceiveTimeout(configuration.jdbc.statementTimeoutInMs milliseconds)

	private def processResultSet(statement: PreparedStatement) = use(statement.getResultSet) { _.size}

	private def logStatement(status: RequestStatus,errorMessage: Option[String] = None) {
		// time measurement is imprecise due to multi-core nature
		// ensure statement execution doesn't start before starting
		statementExecutionStartDate = max(statementExecutionStartDate,executionStartDate)
		// ensure statement execution doesn't end before it starts
		statementExecutionEndDate = max(statementExecutionEndDate,statementExecutionStartDate)
		// ensure execution doesn't end before statement execution ends
		executionEndDate = max(executionEndDate,statementExecutionEndDate)
		// Log request
		if (status == KO)
			debug("Statement failed : " + errorMessage.getOrElse(""))
		DataWriter.logRequest(session.scenarioName,session.userId,statementName,executionStartDate,
			statementExecutionStartDate,statementExecutionEndDate,executionEndDate,status,errorMessage)
	}
}
