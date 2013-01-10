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

import akka.actor.{ ActorRef, Props }

import scalaz._
import Scalaz._

object JdbcTransactionAction {
	def apply(builders: List[AbstractJdbcStatementBuilder[_]],isolationLevel: Option[Int],next: ActorRef) = new JdbcTransactionAction(builders,isolationLevel,next)
}

class JdbcTransactionAction(builders: List[AbstractJdbcStatementBuilder[_]],isolationLevel: Option[Int],val next: ActorRef) extends JdbcAction {

	/**
	 * Core method executed when the Action received a Session message
	 *
	 * @param session the session of the virtual user
	 * @return Nothing
	 */
	def execute(session: Session) {
		def buildBundle(bundleData: (AbstractJdbcStatementBuilder[_],(String,List[Any]))) = {
			val nameAndParams = bundleData._2
			StatementBundle(nameAndParams._1,bundleData._1,nameAndParams._2)
		}
		val resolvedQueries = builders.map(resolveQuery(_,session)).sequence[({ type l[a] = Validation[String, a] })#l, (String,List[Any])]
		resolvedQueries match {
			case Success(resolvedNamesAndParams) =>
				val bundles = builders.zip(resolvedNamesAndParams).map(buildBundle(_))
				val jdbcActor = context.actorOf(Props(JdbcTransactionActor(bundles,isolationLevel,session,next)))
				jdbcActor ! Execute
			case Failure(message) =>
				error(message)
				next ! session
		}
	}
}
