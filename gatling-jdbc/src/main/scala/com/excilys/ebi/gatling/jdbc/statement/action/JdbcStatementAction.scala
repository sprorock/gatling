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

import com.excilys.ebi.gatling.core.action.{Action, Bypass}
import com.excilys.ebi.gatling.core.session.{Expression, Session}
import com.excilys.ebi.gatling.jdbc.statement.builder.AbstractJdbcStatementBuilder

import akka.actor.{Props, ActorRef}
import scalaz._

object JdbcStatementAction {

	def apply(statementName: Expression[String], statementBuilder: AbstractJdbcStatementBuilder[_],isolationLevel: Option[Int],next: ActorRef) =
		new JdbcStatementAction(statementName,statementBuilder,isolationLevel,next)
}

class JdbcStatementAction(statementName: Expression[String], statementBuilder: AbstractJdbcStatementBuilder[_],isolationLevel: Option[Int],val next: ActorRef) extends Action with Bypass {

	/**
	 * Core method executed when the Action received a Session message
	 *
	 * @param session the session of the virtual user
	 * @return Nothing
	 */
	def execute(session: Session) {

		val resolvedStatementName = statementName(session)
		resolvedStatementName match {
			case Success(statementName) =>
				val jdbcActor = context.actorOf(Props(JdbcHandlerActor(statementName,statementBuilder,isolationLevel,session,next)))
				jdbcActor ! ExecuteStatement

			case Failure(message) =>
				error(message)
				next ! session
		}

	}
}
