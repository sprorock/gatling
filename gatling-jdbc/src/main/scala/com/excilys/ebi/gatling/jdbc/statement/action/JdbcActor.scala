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

import java.sql.{ Connection, PreparedStatement }

import scala.math.max

import com.excilys.ebi.gatling.core.action.BaseActor
import com.excilys.ebi.gatling.core.config.GatlingConfiguration.configuration
import com.excilys.ebi.gatling.core.result.message.{ KO, RequestStatus }
import com.excilys.ebi.gatling.core.result.writer.DataWriter
import com.excilys.ebi.gatling.core.session.Session
import com.excilys.ebi.gatling.core.util.IOHelper.use
import com.excilys.ebi.gatling.core.util.TimeHelper.nowMillis
import com.excilys.ebi.gatling.jdbc.util.ConnectionFactory
import com.excilys.ebi.gatling.jdbc.util.RowIterator.ResultSet2RowIterator

import akka.actor.ActorRef
import akka.util.duration.intToDurationInt

abstract class JdbcActor(session: Session,next: ActorRef) extends BaseActor {

	var currentStatementName: String = _

	var executionStartDate = 0L
	var statementExecutionStartDate = 0L
	var statementExecutionEndDate = 0L
	var executionEndDate = 0L

	def setupConnection(isolationLevel: Option[Int]) = {
		val connection = ConnectionFactory.getConnection
		if (isolationLevel.isDefined) connection.setTransactionIsolation(isolationLevel.get)
		connection
	}

	def execute

	def processResultSet(statement: PreparedStatement) = use(statement.getResultSet) { _.size}

	def closeStatement(statement: PreparedStatement) { if (statement != null) statement.close }

	def closeConnection(connection: Connection) { if (connection != null) connection.close}

	def executeNext(newSession: Session) = next ! newSession.increaseTimeShift(nowMillis - executionEndDate)

	def resetTimeout = context.setReceiveTimeout(configuration.jdbc.statementTimeoutInMs milliseconds)

	def logStatement(status: RequestStatus,errorMessage: Option[String] = None) {
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
		DataWriter.logRequest(session.scenarioName,session.userId,currentStatementName,executionStartDate,
			statementExecutionStartDate,statementExecutionEndDate,executionEndDate,status,errorMessage)
	}
}
