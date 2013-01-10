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

import com.excilys.ebi.gatling.core.session.Session
import com.excilys.ebi.gatling.jdbc.statement.builder.AbstractJdbcStatementBuilder
import com.excilys.ebi.gatling.jdbc.util.StatementBundle

import akka.actor.{ Props, ActorRef }
import scalaz.{ Failure, Success }

object JdbcStatementAction {

	def apply(builder: AbstractJdbcStatementBuilder[_],next: ActorRef) = new JdbcStatementAction(builder,next)
}

class JdbcStatementAction(builder: AbstractJdbcStatementBuilder[_],val next: ActorRef) extends JdbcAction {

	/**
	 * Core method executed when the Action received a Session message
	 *
	 * @param session the session of the virtual user
	 * @return Nothing
	 */
	def execute(session: Session) {
		resolveQuery(builder,session) match {
			case Success((statementName,paramsList)) =>
				val bundle = StatementBundle(statementName,builder,paramsList)
				val jdbcActor = context.actorOf(Props(JdbcStatementActor(bundle,session,next)))
				jdbcActor ! Execute

			case Failure(message) =>
				error(message)
				next ! session
		}

	}
}
