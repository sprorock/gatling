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

import java.sql.{ Connection, PreparedStatement, SQLException }

import com.excilys.ebi.gatling.core.session.Session
import com.excilys.ebi.gatling.core.structure.ChainBuilder
import com.excilys.ebi.gatling.jdbc.statement.builder.AbstractJdbcStatementBuilder
import com.excilys.ebi.gatling.jdbc.util.JdbcSession.session2JdbcSession

object JdbcHelper {

	def withTransaction(session: Session)(block : ChainBuilder): Session =
		try {
			session.beginTransaction
			session.getConnectionFromSession.setAutoCommit(false)
			// TODO : execute chain, how ?
			session.getConnectionFromSession.commit
			session
		} catch {
			case sqle: SQLException =>
				session.getConnectionFromSession.rollback
				session
		} finally {
			session.getConnectionFromSession.close
			session.endTransaction
		}

	def getConnection(session : Session) =
		if (session.isInTransaction)
			session.getConnectionFromSession
		else
			ConnectionFactory.getConnection

	def closeConnection(session: Session,connection: Connection) =
		if (!session.isInTransaction && connection != null)
			connection.close

	def getStatement(session: Session,connection: Connection, builder: AbstractJdbcStatementBuilder[_]) = {
		if (session.isInBatchUpdate) {
			if (session.isStatementInCache) {
				session.getStatementFromSession
			} else {
				val statement = builder.build(connection)
				session.cacheStatement(statement)
				statement
			}
		} else {
			builder.build(connection)
		}
	}

	def closeStatement(session: Session,statement: PreparedStatement) =
		if (!session.isInBatchUpdate && statement != null)
			statement.close
}
