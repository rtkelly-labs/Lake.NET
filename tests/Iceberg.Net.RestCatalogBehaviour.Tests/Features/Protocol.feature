Feature: Protocol
  What any REST client can rely on at the HTTP level: the configuration lists the routes the server serves, a table
  load can be revalidated cheaply, and every error has the same body shape and the documented status.

  @spec:rest:getconfig
  Scenario: The configuration advertises the routes the server serves
    When I fetch the catalog configuration
    Then the configuration advertises the endpoints:
      | endpoint                                                           |
      | GET /v1/{prefix}/namespaces                                        |
      | POST /v1/{prefix}/namespaces                                       |
      | POST /v1/{prefix}/namespaces/{namespace}/properties                |
      | GET /v1/{prefix}/namespaces/{namespace}/tables                     |
      | GET /v1/{prefix}/namespaces/{namespace}/tables/{table}             |
      | POST /v1/{prefix}/namespaces/{namespace}/tables/{table}            |
      | DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}          |
      | POST /v1/{prefix}/namespaces/{namespace}/register                  |
      | POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister |
      | POST /v1/{prefix}/tables/rename                                    |
      | POST /v1/{prefix}/transactions/commit                              |
      | GET /v1/{prefix}/namespaces/{namespace}/views/{view}               |
      | POST /v1/{prefix}/views/rename                                     |
    And the configuration has "defaults" and "overrides" objects

  @spec:rest:loadtable
  Scenario: A table load carries an ETag, and revalidating an unchanged table answers 304
    Given the namespace "sales"
    And the table "sales.orders"
    When I load "sales.orders" over HTTP
    Then the response has an ETag
    When I load "sales.orders" over HTTP again, sending that ETag as If-None-Match
    Then the response is 304 with no body

  @spec:rest:loadtable @spec:rest:updatetable
  Scenario: After a commit the ETag changes and the old one no longer matches
    Given the namespace "sales"
    And the table "sales.orders"
    When I load "sales.orders" over HTTP
    And I append to "sales.orders":
      | id | name | region |
      | 1  | a    | eu     |
    And I load "sales.orders" over HTTP again, sending that ETag as If-None-Match
    Then the response is 200 with the table's metadata
    And the response has an ETag different from the first

  @spec:rest:loadtable @spec:rest:createnamespace @spec:rest:dropnamespace @spec:rest:createtable @spec:rest:updatetable @spec:rest:loadview @spec:rest:updateproperties
  Scenario Outline: Errors carry the error model with the documented status and type
    Given the namespace "sales"
    And the table "sales.orders"
    When I send a request that <mistake>
    Then the request is refused with <status> "<type>"

    Examples:
      | mistake                                                     | status | type                         |
      | loads the missing table "sales.invoices"                    | 404    | NoSuchTableException         |
      | loads a table in the missing namespace "marketing"          | 404    | NoSuchNamespaceException     |
      | loads the missing view "sales.stale"                        | 404    | NoSuchViewException          |
      | creates the existing namespace "sales"                      | 409    | AlreadyExistsException       |
      | creates the existing table "sales.orders"                   | 409    | AlreadyExistsException       |
      | drops the non-empty namespace "sales"                       | 409    | NamespaceNotEmptyException   |
      | commits to "sales.orders" requiring a snapshot it never had | 409    | CommitFailedException        |
      | creates a namespace with malformed JSON                     | 400    | BadRequestException          |
      | creates a table without a schema                            | 400    | BadRequestException          |
      | sets and removes the property "owner" of "sales"            | 422    | UnprocessableEntityException |
