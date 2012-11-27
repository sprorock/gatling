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
package com.excilys.ebi.gatling.jdbc.util

import com.excilys.ebi.gatling.core.session.Session
import com.excilys.ebi.gatling.core.session.Session.GATLING_PRIVATE_ATTRIBUTE_PREFIX
import java.sql.{ Connection,PreparedStatement }

object JdbcSession {

	val IN_TRANSACTION = GATLING_PRIVATE_ATTRIBUTE_PREFIX + "jdbc.inTransaction"

	val IN_BATCH_UPDATE = GATLING_PRIVATE_ATTRIBUTE_PREFIX + "jdbc.inBatchUpdate"

	val STATEMENT_CACHE = GATLING_PRIVATE_ATTRIBUTE_PREFIX + "jdbc.statementCache"

	implicit def session2JdbcSession(session: Session) = new JdbcSession(session)
}

// TODO : use an implicit class after upgrade to Scala 2.10
class JdbcSession(session: Session) {

	def beginTransaction = session.set(JdbcSession.IN_TRANSACTION,ConnectionFactory.getConnection)

	def endTransaction = session.remove(JdbcSession.IN_TRANSACTION)

	def isInTransaction = session.contains(JdbcSession.IN_TRANSACTION)

	def getConnectionFromSession = session.getAs[Connection](JdbcSession.IN_TRANSACTION).get

	def beginBatchUpdate = session.set(JdbcSession.IN_BATCH_UPDATE,"")

	def endBatchUpdate = session.remove(JdbcSession.IN_BATCH_UPDATE)

	def isInBatchUpdate = session.contains(JdbcSession.IN_BATCH_UPDATE)

	def getStatementFromSession = session.getAs[PreparedStatement](JdbcSession.STATEMENT_CACHE).get

	def cacheStatement(statement: PreparedStatement) = session.set(JdbcSession.STATEMENT_CACHE,statement)

	def isStatementInCache = session.contains(JdbcSession.STATEMENT_CACHE)
}
