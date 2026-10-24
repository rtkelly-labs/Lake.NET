Feature: Views
  A view is a named, versioned SQL definition with an output schema. Views live in namespaces beside tables and share
  their names: a name is a table or a view, never both.

  Background:
    Given the namespace "sales"

  @spec:rest:createview @spec:rest:loadview
  Scenario: A created view is loaded with its SQL and schema
    When I create the view "sales.recent" as "SELECT * FROM sales.orders WHERE day > current_date - 7"
    Then the view "sales.recent" is at version 1 with SQL "SELECT * FROM sales.orders WHERE day > current_date - 7"
    And the view "sales.recent" has the columns "id", "name" and "region"

  @spec:rest:replaceview @spec:rest:loadview
  Scenario: Replacing a view makes the new definition current and keeps the old version in its history
    Given the view "sales.recent" as "SELECT * FROM sales.orders"
    When I replace the view "sales.recent" with "SELECT id FROM sales.orders"
    Then the view "sales.recent" is at version 2 with SQL "SELECT id FROM sales.orders"
    And the view "sales.recent" keeps 2 versions

  @spec:rest:listviews @spec:rest:viewexists
  Scenario: Views are listed per namespace and checked by name, apart from tables
    Given the view "sales.recent" as "SELECT * FROM sales.orders"
    And the view "sales.big" as "SELECT * FROM sales.orders WHERE amount > 100"
    And the table "sales.orders"
    Then the views of "sales" are "big" and "recent"
    And the tables of "sales" are "orders"
    And the view "sales.recent" exists
    And the view "sales.stale" does not exist

  @spec:rest:renameview @spec:rest:viewexists
  Scenario: A renamed view keeps its definition under its new name, across namespaces too
    Given the namespace "archive"
    And the view "sales.recent" as "SELECT * FROM sales.orders"
    When I rename the view "sales.recent" to "archive.recent_orders"
    Then the view "sales.recent" does not exist
    And the view "archive.recent_orders" is at version 1 with SQL "SELECT * FROM sales.orders"

  @spec:rest:dropview @spec:rest:viewexists
  Scenario: A dropped view is gone
    Given the view "sales.recent" as "SELECT * FROM sales.orders"
    When I drop the view "sales.recent"
    Then the view "sales.recent" does not exist
    And the views of "sales" are none

  @spec:rest:loadview
  Scenario: Loading a view that does not exist fails
    When I try to load the view "sales.stale"
    Then the attempt fails because the view does not exist

  @spec:rest:createview @spec:rest:createtable
  Scenario: A view cannot take the name of a table
    Given the table "sales.orders"
    When I try to create the view "sales.orders" as "SELECT 1"
    Then the attempt fails because it already exists
    And the table "sales.orders" exists
    And the view "sales.orders" does not exist

  @spec:rest:createtable @spec:rest:createview
  Scenario: A table cannot take the name of a view
    Given the view "sales.recent" as "SELECT * FROM sales.orders"
    When I try to create the table "sales.recent"
    Then the attempt fails because it already exists
    And the view "sales.recent" exists
    And the table "sales.recent" does not exist

  @spec:rest:renameview @spec:rest:renametable
  Scenario: Neither a view nor a table can be renamed onto the other's name
    Given the view "sales.recent" as "SELECT * FROM sales.orders"
    And the table "sales.orders"
    When I try to rename the view "sales.recent" to "sales.orders"
    Then the attempt fails because it already exists
    When I try to rename "sales.orders" to "sales.recent"
    Then the attempt fails because it already exists
    And the view "sales.recent" exists
    And the table "sales.orders" exists
