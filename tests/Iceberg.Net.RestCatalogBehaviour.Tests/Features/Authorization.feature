Feature: Authentication and roles
  With authentication configured, every request but the token request needs a bearer token. What an authenticated
  principal may then do follows its grants: read < write < admin on a namespace and everything beneath it. A refusal
  is a 403 and changes nothing.

  Background:
    Given the server requires authentication, with these principals and grants:
      | principal | grant       |
      | root      | admin:*     |
      | admin     | admin:sales |
      | writer    | write:sales |
      | reader    | read:sales  |
      | nobody    |             |
    And the namespace "sales"
    And the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |

  @spec:rest:listnamespaces
  Scenario Outline: A request without a valid bearer token is not authenticated
    When I list the namespaces <credentials>
    Then the request is refused with 401 "NotAuthorizedException"

    Examples:
      | credentials                          |
      | without a token                      |
      | with a token the server never issued |

  @spec:rest:gettoken
  Scenario: A client with a wrong secret gets no token
    When "writer" asks for a token with the secret "not-the-secret"
    Then the token request is refused with 401 "invalid_client"

  @spec:rest:gettoken @spec:rest:getconfig @spec:rest:loadtable
  Scenario: A client with valid credentials gets a token and uses it
    When "reader" connects with its client credentials
    Then "reader" can load "sales.orders"

  @spec:rest:listtables @spec:rest:loadtable @spec:rest:updatetable @spec:rest:createtable @spec:rest:droptable
  Scenario Outline: What each role may do with the tables of a namespace
    When "<principal>" tries to <operation>
    Then the attempt is <outcome>

    Examples:
      | principal | operation                        | outcome   |
      | admin     | list the tables of "sales"       | allowed   |
      | admin     | load "sales.orders"              | allowed   |
      | admin     | append a row to "sales.orders"   | allowed   |
      | admin     | create the table "sales.fresh"   | allowed   |
      | admin     | drop the table "sales.orders"    | allowed   |
      | writer    | list the tables of "sales"       | allowed   |
      | writer    | load "sales.orders"              | allowed   |
      | writer    | append a row to "sales.orders"   | allowed   |
      | writer    | create the table "sales.fresh"   | allowed   |
      | writer    | drop the table "sales.orders"    | forbidden |
      | reader    | list the tables of "sales"       | allowed   |
      | reader    | load "sales.orders"              | allowed   |
      | reader    | append a row to "sales.orders"   | forbidden |
      | reader    | create the table "sales.fresh"   | forbidden |
      | reader    | drop the table "sales.orders"    | forbidden |
      | nobody    | list the tables of "sales"       | forbidden |
      | nobody    | load "sales.orders"              | forbidden |
      | nobody    | append a row to "sales.orders"   | forbidden |
      | nobody    | create the table "sales.fresh"   | forbidden |
      | nobody    | drop the table "sales.orders"    | forbidden |

  @spec:rest:loadtable
  Scenario: A refused principal cannot tell a missing table from a hidden one
    When "nobody" tries to load "sales.invoices"
    Then the attempt is forbidden

  @spec:rest:listnamespaces @spec:rest:listtables
  Scenario: Lists only show what the principal may see
    Given the namespace "hr"
    And the table "hr.people"
    Then "reader" sees the top-level namespaces "sales"
    And "nobody" sees no top-level namespaces
    And "root" sees the top-level namespaces "hr" and "sales"
