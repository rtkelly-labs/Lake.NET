@spec:rest:updatetable @spec:rest:loadtable
Feature: Commits
  The server owns commits: a client sends requirements and updates, and the server checks every requirement against
  the table's current metadata before it applies anything. A requirement that no longer holds refuses the whole commit.

  Background:
    Given the namespace "sales"

  Scenario: Appended rows are read back by another client
    Given the table "sales.orders"
    When I append to "sales.orders":
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
    Then a fresh client reads exactly these rows from "sales.orders":
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
    And "sales.orders" has 1 snapshot

  Scenario: Two writers that loaded the same version both land, the later one on top of the earlier
    Given the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
    And two writers that have both loaded "sales.orders"
    When the first writer appends:
      | id | name  | region |
      | 2  | first | eu     |
    And the second writer appends:
      | id | name   | region |
      | 3  | second | us     |
    Then the second writer needed 2 attempts
    And the table "sales.orders" holds exactly:
      | id | name   | region |
      | 1  | a      | eu     |
      | 2  | first  | eu     |
      | 3  | second | us     |
    And the current snapshot of "sales.orders" is the second writer's, whose parent is the first writer's

  Scenario: Two writers racing at the same moment both land
    Given the table "sales.orders"
    When 4 writers that loaded the same version of "sales.orders" each append 5 rows at once
    Then "sales.orders" holds 20 rows
    And "sales.orders" has 4 snapshots in one line of history

  Scenario Outline: A commit based on stale knowledge of the table is refused and the table is unchanged
    Given the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
    And a writer that has loaded "sales.orders"
    And meanwhile <change>
    When the writer commits a property change that requires <requirement>
    Then the commit fails with CommitFailed
    And "sales.orders" has not changed since the meanwhile change

    Examples:
      | change                                                 | requirement                                  |
      | another client appended to "sales.orders"              | the main branch to be at the snapshot it saw |
      | another client added a column "note" to "sales.orders" | the current schema to be the one it saw      |

  Scenario: A commit whose requirements still hold is applied
    Given the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
    And a writer that has loaded "sales.orders"
    When the writer commits a property change that requires the main branch to be at the snapshot it saw
    Then the property "owner" of "sales.orders" is "writer"

  Scenario: After schema evolution, time travel to an older snapshot reads it in the schema it was written with
    Given the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
    And I remember the current snapshot of "sales.orders" as "before"
    When I add the optional string column "note" to "sales.orders"
    And I append to "sales.orders":
      | id | name | region | note |
      | 2  | b    | us     | late |
    Then reading "sales.orders" as of snapshot "before" gives the columns "id", "name" and "region" and the rows:
      | id | name | region |
      | 1  | a    | eu     |
    And reading "sales.orders" now gives the columns "id", "name", "region" and "note" and the rows:
      | id | name | region | note   |
      | 1  | a    | eu     | <null> |
      | 2  | b    | us     | late   |
