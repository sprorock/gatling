/**
 * Copyright 2011-2012 eBusiness Information, Groupe Excilys (www.excilys.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.excilys.ebi.gatling.jdbc.config

import org.apache.tomcat.jdbc.pool.DataSource

import com.excilys.ebi.gatling.jdbc.util.ConnectionFactory

import grizzled.slf4j.Logging

object JdbcProtocolConfigurationBuilder {

	private[gatling] val BASE_JDBC_PROTOCOL_CONFIGURATION_BUILDER = new JdbcProtocolConfigurationBuilder(Attributes(properties = Map[String, Any]()))

	def jdbcConfig = BASE_JDBC_PROTOCOL_CONFIGURATION_BUILDER
}

private case class Attributes(
	url: String = "",
	driver: Option[String] = None,
	username: String = "",
	password: String = "",
	initial: Option[Int] = None,
	minIdle: Option[Int] = None,
	maxActive: Option[Int] = None,
	maxIdle: Option[Int] = None,
	maxWait: Option[Int] = None,
	defaultTransactionIsolation: Option[Int] = None,
	defaultCatalog: Option[String] = None,
	defaultReadOnly: Option[java.lang.Boolean] = None,
	initSQL: Option[String] = None,
	properties: Map[String, Any])

class JdbcProtocolConfigurationBuilder(attributes: Attributes) extends Logging {

	def url(url: String) = new JdbcProtocolConfigurationBuilder(attributes.copy(url = url))

	def driver(driver: String) = new JdbcProtocolConfigurationBuilder(attributes.copy(driver = Some(driver)))

	def username(username: String) = new JdbcProtocolConfigurationBuilder(attributes.copy(username = username))

	def password(password: String) = new JdbcProtocolConfigurationBuilder(attributes.copy(password = password))

	def properties(properties: Map[String, Any]) = new JdbcProtocolConfigurationBuilder(attributes.copy(properties = properties))

	def initial(initial: Int) = new JdbcProtocolConfigurationBuilder(attributes.copy(initial = Some(initial)))

	def minIdle(minIdle: Int) = new JdbcProtocolConfigurationBuilder(attributes.copy(minIdle = Some(minIdle)))

	def maxActive(maxActive: Int) = new JdbcProtocolConfigurationBuilder(attributes.copy(maxActive = Some(maxActive)))

	def maxIdle(maxIdle: Int) = new JdbcProtocolConfigurationBuilder(attributes.copy(maxIdle = Some(maxIdle)))

	def maxWait(maxWait: Int) = new JdbcProtocolConfigurationBuilder(attributes.copy(maxWait = Some(maxWait)))

	def initSQL(sql: String) = new JdbcProtocolConfigurationBuilder(attributes.copy(initSQL = Some(sql)))

	def defaultTransactionIsolation(isolationLevel: Int) = new JdbcProtocolConfigurationBuilder(attributes.copy(defaultTransactionIsolation = Some(isolationLevel)))

	def defaultReadOnly(readOnly: Boolean) = new JdbcProtocolConfigurationBuilder(attributes.copy(defaultReadOnly = Some(readOnly)))

	def defaultCatalog(catalog: String) = new JdbcProtocolConfigurationBuilder(attributes.copy(defaultCatalog = Some(catalog)))

	private[jdbc] def build = {
		if(attributes.url == "")
			throw new IllegalArgumentException("JDBC connection URL is not defined.")
		if(attributes.username == "")
			throw new IllegalArgumentException("User is not defined.")
		if(attributes.password == "")
			throw new IllegalArgumentException("Password is not defined.")
		ConnectionFactory.setDataSource(setupDataSource)
		JdbcProtocolConfiguration
	}

	private def setupDataSource: DataSource = {
		val ds = new DataSource
		ds.setUrl(attributes.url)
		ds.setUsername(attributes.username)
		ds.setPassword(attributes.password)
		ds.setConnectionProperties(buildPropertiesString(attributes.properties))
		ds.setDefaultAutoCommit(true)
		callIfSome(attributes.driver,ds.setDriverClassName)
		callIfSome(attributes.initial,ds.setInitialSize)
		callIfSome(attributes.minIdle,ds.setMinIdle)
		callIfSome(attributes.maxActive,ds.setMaxActive)
		callIfSome(attributes.maxIdle,ds.setMaxIdle)
		callIfSome(attributes.maxWait,ds.setMaxWait)
		callIfSome(attributes.initSQL,ds.setInitSQL)
		callIfSome(attributes.defaultTransactionIsolation,ds.setDefaultTransactionIsolation)
		callIfSome(attributes.defaultReadOnly,ds.setDefaultReadOnly)
		callIfSome(attributes.defaultCatalog,ds.setDefaultCatalog)
		ds
	}

	private def buildPropertiesString(map: Map[String, Any]) = map.map { case (key, value) => key + "=" + value }.mkString(";")

	private def callIfSome[T](value: Option[T],f: T => Unit) { if(value.isDefined) f(value.get) }
}
