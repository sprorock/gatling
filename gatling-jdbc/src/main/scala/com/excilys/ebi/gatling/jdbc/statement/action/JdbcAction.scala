package com.excilys.ebi.gatling.jdbc.statement.action

import com.excilys.ebi.gatling.core.action.{ Action, Bypass }
import com.excilys.ebi.gatling.core.session.Session
import com.excilys.ebi.gatling.jdbc.statement.builder.AbstractJdbcStatementBuilder

abstract class JdbcAction extends Action with Bypass {

	def resolveQuery(builder: AbstractJdbcStatementBuilder[_],session: Session) = {
		for {
			name <- builder.statementName(session)
			paramsList <- builder.resolveParams(session)
		} yield (name,paramsList)
	}
}
