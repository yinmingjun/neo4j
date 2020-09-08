/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.config.Setting

class CommunityPrivilegeAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(GraphDatabaseSettings.auth_enabled -> java.lang.Boolean.TRUE)

  // Tests for showing privileges

  test("should fail on showing privileges from community") {
    assertFailure("SHOW ALL PRIVILEGES", "Unsupported administration command: SHOW ALL PRIVILEGES")
  }

  test("should fail on showing role privileges from community") {
    assertFailure("SHOW ROLE reader PRIVILEGES", "Unsupported administration command: SHOW ROLE reader PRIVILEGES")
    assertFailure("SHOW ROLE $role PRIVILEGES", "Unsupported administration command: SHOW ROLE $role PRIVILEGES")
    assertFailure("SHOW ROLES role1, $role2 PRIVILEGES", "Unsupported administration command: SHOW ROLES role1, $role2 PRIVILEGES")
  }

  test("should fail on showing user privileges for non-existing user with correct error message") {
    assertFailure("SHOW USER foo PRIVILEGES", "Unsupported administration command: SHOW USER foo PRIVILEGES")
    assertFailure("SHOW USER $foo PRIVILEGES", "Unsupported administration command: SHOW USER $foo PRIVILEGES")
    assertFailure("SHOW USERS $foo, bar PRIVILEGES", "Unsupported administration command: SHOW USERS $foo, bar PRIVILEGES")
  }

  private val privilegeTypes = Seq(
    ("GRANT", "TO"),
    ("REVOKE", "FROM"),
    ("REVOKE GRANT", "FROM"),
    ("DENY", "TO"),
    ("REVOKE DENY", "FROM")
  )

  privilegeTypes.foreach {
    case (privilegeType, preposition) =>
      test(s"should fail on $privilegeType from community") {
        val command = s"$privilegeType TRAVERSE ON GRAPH * NODES * (*) $preposition custom"
        assertFailure(command, s"Unsupported administration command: $command")
      }
  }
}
