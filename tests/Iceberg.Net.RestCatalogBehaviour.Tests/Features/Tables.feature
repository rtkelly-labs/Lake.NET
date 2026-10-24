Feature: Tables
  A table is a name in a namespace pointing at a metadata file. The catalog creates, finds, lists, renames, drops,
  registers and unregisters tables; dropping with purge also removes the table's files.

  Background:
    Given the namespace "sales"

  @spec:rest:createtable @spec:rest:loadtable
  Scenario: A created table has the requested schema and partitioning and no snapshot yet
    When I create the table "sales.orders" partitioned by "region"
    Then the table "sales.orders" exists
    And loading "sales.orders" gives the columns "id", "name" and "region"
    And loading "sales.orders" gives a partition spec on "region" with transform "identity"
    And "sales.orders" has no current snapshot

  @spec:rest:createtable @spec:rest:listtables @spec:rest:tableexists
  Scenario: Tables are listed per namespace, and existence is checked by name
    Given the namespace "marketing"
    And the tables "sales.orders", "sales.returns" and "marketing.leads"
    Then the tables of "sales" are "orders" and "returns"
    And the tables of "marketing" are "leads"
    And the table "sales.orders" exists
    And the table "sales.invoices" does not exist

  @spec:rest:listtables
  Scenario: Tables are listed a page at a time when the caller asks for small pages
    Given the tables "sales.t1", "sales.t2" and "sales.t3"
    When I list the tables of "sales" 2 at a time
    Then the pages hold 2 and 1 tables
    And together the pages list "t1", "t2" and "t3" in that order

  @spec:rest:renametable @spec:rest:loadtable
  Scenario Outline: A renamed table keeps its data under its new name
    Given the namespace "archive"
    And the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
    When I rename "sales.orders" to "<destination>"
    Then the table "sales.orders" does not exist
    And the table "<destination>" holds exactly:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |

    Examples:
      | destination      |
      | sales.orders_old |
      | archive.orders   |

  @spec:rest:renametable
  Scenario: A table cannot be renamed onto a name that is taken
    Given the tables "sales.orders" and "sales.returns"
    When I try to rename "sales.orders" to "sales.returns"
    Then the attempt fails because it already exists
    And the table "sales.orders" exists

  @spec:rest:droptable @spec:rest:tableexists
  Scenario: A dropped table is gone from the catalog but its files stay
    Given the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
    When I drop the table "sales.orders"
    Then the table "sales.orders" does not exist
    And the tables of "sales" are none
    And the data and metadata files of "sales.orders" still exist

  @spec:rest:droptable
  Scenario: A table dropped with purge loses its data and metadata files too
    Given the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
    And "sales.orders" also holds:
      | id | name | region |
      | 3  | c    | eu     |
    When I drop the table "sales.orders" with purge
    Then the table "sales.orders" does not exist
    And none of the data or metadata files of "sales.orders" exist

  @spec:rest:registertable @spec:rest:unregistertable @spec:rest:loadtable
  Scenario: An unregistered table's metadata file can be registered under a new name
    Given the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
    When I unregister "sales.orders"
    And I register the last metadata file of "sales.orders" as "sales.orders_restored"
    Then the table "sales.orders_restored" holds exactly:
      | id | name | region |
      | 1  | a    | eu     |

  @spec:rest:registertable
  Scenario: A metadata file cannot be registered under a name that is taken
    Given the tables "sales.orders" and "sales.returns"
    When I try to register the current metadata file of "sales.orders" as "sales.returns"
    Then the attempt fails because it already exists

  @spec:rest:unregistertable @spec:rest:registertable
  Scenario: Unregistering forgets the table but keeps its files, so it can be registered again
    Given the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
    When I unregister "sales.orders"
    Then the response carries the last metadata file of "sales.orders"
    And the table "sales.orders" does not exist
    And the data and metadata files of "sales.orders" still exist
    When I register the last metadata file of "sales.orders" as "sales.orders"
    Then the table "sales.orders" holds exactly:
      | id | name | region |
      | 1  | a    | eu     |

  @spec:rest:loadtable
  Scenario: Loading a table that does not exist fails
    When I try to load the table "sales.invoices"
    Then the attempt fails because the table does not exist

  @spec:rest:createtable
  Scenario: Creating a table that already exists is refused
    Given the table "sales.orders" holding:
      | id | name | region |
      | 1  | a    | eu     |
    When I try to create the table "sales.orders"
    Then the attempt fails because it already exists
    And the table "sales.orders" holds exactly:
      | id | name | region |
      | 1  | a    | eu     |

  @spec:rest:createtable @spec:rest:loadtable
  Scenario: Tables cannot be created in or loaded from a namespace that does not exist
    When I try to create the table "marketing.leads"
    Then the attempt fails because the namespace does not exist
    When I try to load the table "marketing.leads"
    Then the attempt fails because the namespace does not exist

  @spec:rest:droptable
  Scenario: Dropping a table that does not exist fails
    When I try to drop the table "sales.invoices"
    Then the attempt fails because the table does not exist
