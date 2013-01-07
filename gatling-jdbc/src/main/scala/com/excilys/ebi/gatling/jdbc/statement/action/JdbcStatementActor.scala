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

import com.excilys.ebi.gatling.core.result.message.{ KO, OK }
import com.excilys.ebi.gatling.core.session.Session
import com.excilys.ebi.gatling.core.util.TimeHelper.{ computeTimeMillisFromNanos, nowMillis }

import akka.actor.{ ActorRef, ReceiveTimeout }
import com.excilys.ebi.gatling.jdbc.util.StatementBundle

// Message to start execution of query by the actor
object ExecuteStatement

object JdbcStatementActor {

	def apply(statementName: String,bundle: StatementBundle,session: Session,next: ActorRef) =
		new JdbcStatementActor(statementName,bundle,session,next)
}

class JdbcStatementActor(
	statementName: String,
	bundle: StatementBundle,
	session: Session,
	next: ActorRef) extends JdbcActor(session,next) {

	def receive = {
		case ExecuteStatement => execute
		case ReceiveTimeout =>
			logStatement(statementName,KO,Some("JdbcHandlerActor timed out"))
			executeNext(session.setFailed)
	}

	override def execute = {

		var statement: PreparedStatement = null
		var connection: Connection = null
		try {
			executionStartDate = nowMillis
			// Fetch connection
			connection = setupConnection(None)
			resetTimeout
			// Execute statement
			statement = bundle.buildStatement(connection)
			statementExecutionStartDate = computeTimeMillisFromNanos(nanoTime)
			val hasResultSet = statement.execute
			statementExecutionEndDate = computeTimeMillisFromNanos(nanoTime)
			resetTimeout
			// Process result set
			if (hasResultSet) processResultSet(statement)
			executionEndDate = computeTimeMillisFromNanos(nanoTime)
			logStatement(statementName,OK)
			next ! session
			context.stop(self)

		} catch {
			case e : SQLException =>
				logStatement(statementName,KO,Some(e.getMessage))
				executeNext(session.setFailed)
		} finally {
			closeStatement(statement)
			closeConnection(connection)
		}
	}

}
