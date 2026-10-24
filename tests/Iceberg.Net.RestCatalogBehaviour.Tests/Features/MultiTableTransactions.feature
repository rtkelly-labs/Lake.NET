@spec:rest:committransaction
Feature: Multi-table transactions
  A transaction changes several tables at once. Every table's requirements are checked first; if any fails, no
  table changes at all.

  Background:
    Given the namespace "sales"
    And the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
    And the table "sales.customers" holding:
      | id | name | region |
      | 1  | c    | eu     |

  Scenario: A transaction whose requirements all hold changes every table
    Given I remember the state of "sales.orders" and "sales.customers"
    When I commit one transaction that sets "batch" to "42" on "sales.orders" and "sales.customers", requiring each to be at the snapshot I saw
    Then the property "batch" of "sales.orders" is "42"
    And the property "batch" of "sales.customers" is "42"

  Scenario: A transaction appends to several tables at once
    When I commit one transaction that appends a row with id 7 to "sales.orders" and to "sales.customers"
    Then "sales.orders" holds 2 rows
    And "sales.customers" holds 2 rows

  Scenario Outline: When one table's requirement fails, no table changes
    Given I remember the state of "sales.orders" and "sales.customers"
    And meanwhile <change>
    When I commit one transaction that sets "batch" to "42" on "sales.orders" and "sales.customers", requiring each to be <requirement>
    Then the commit fails with CommitFailed
    And "sales.orders" and "sales.customers" have no property "batch"
    And "<other table>" has not changed since I remembered it

    Examples:
      | change                                                    | other table     | requirement             |
      | another client appended to "sales.orders"                 | sales.customers | at the snapshot I saw   |
      | another client appended to "sales.customers"              | sales.orders    | at the snapshot I saw   |
      | another client added a column "note" to "sales.orders"    | sales.customers | on the schema I saw     |
      | another client added a column "note" to "sales.customers" | sales.orders    | on the schema I saw     |

  Scenario: A transaction naming a table that does not exist changes nothing
    Given I remember the state of "sales.orders" and "sales.customers"
    When I commit one transaction that sets "batch" to "42" on "sales.orders" and on the missing table "sales.invoices"
    Then the attempt fails because the table does not exist
    And "sales.orders" has not changed since I remembered it
