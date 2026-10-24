Feature: Namespaces
  A REST catalog organises tables and views into namespaces, which may nest to any depth and carry string properties.
  A namespace can only be dropped once nothing lives in it.

  @spec:rest:createnamespace @spec:rest:loadnamespacemetadata
  Scenario: A created namespace exists and keeps its properties
    When I create the namespace "sales" with properties:
      | key   | value    |
      | owner | data-eng |
    Then the namespace "sales" exists
    And the namespace "sales" has exactly the properties:
      | key   | value    |
      | owner | data-eng |

  @spec:rest:createnamespace @spec:rest:listnamespaces @spec:rest:loadnamespacemetadata
  Scenario: Namespaces nest to several levels and list only their direct children
    Given the namespaces "sales", "sales.eu", "sales.eu.raw" and "sales.us"
    Then the top-level namespaces are "sales"
    And the children of "sales" are "sales.eu" and "sales.us"
    And the children of "sales.eu" are "sales.eu.raw"
    And the namespace "sales.eu.raw" exists

  @spec:rest:listnamespaces
  Scenario: Namespaces are listed a page at a time when the caller asks for small pages
    Given the namespaces "a", "b", "c", "d" and "e"
    When I list the top-level namespaces 2 at a time
    Then the pages hold 2, 2 and 1 namespaces
    And together the pages list "a", "b", "c", "d" and "e" in that order

  @spec:rest:namespaceexists
  Scenario: Existence of a namespace can be checked without loading it
    Given the namespace "sales"
    Then checking whether "sales" exists answers 204
    And checking whether "marketing" exists answers 404

  @spec:rest:updateproperties @spec:rest:loadnamespacemetadata
  Scenario: Updating properties reports what was set, what was removed and what was not there
    Given the namespace "sales" with properties:
      | key   | value    |
      | owner | data-eng |
      | tier  | gold     |
    When I update the properties of "sales", setting:
      | key     | value   |
      | owner   | finance |
      | retains | 30d     |
    And removing "tier" and "ghost"
    Then the update reports updated "owner" and "retains", removed "tier" and missing "ghost"
    And the namespace "sales" has exactly the properties:
      | key     | value   |
      | owner   | finance |
      | retains | 30d     |

  @spec:rest:updateproperties
  Scenario: A property cannot be set and removed in the same update
    Given the namespace "sales" with properties:
      | key   | value    |
      | owner | data-eng |
    When I update the properties of "sales", setting:
      | key   | value   |
      | owner | finance |
    And removing "owner"
    Then the request is refused with 422 "UnprocessableEntityException"
    And the namespace "sales" has exactly the properties:
      | key   | value    |
      | owner | data-eng |

  @spec:rest:dropnamespace @spec:rest:loadnamespacemetadata
  Scenario: An empty namespace can be dropped
    Given the namespace "sales"
    When I drop the namespace "sales"
    Then the namespace "sales" does not exist
    And the top-level namespaces are none

  @spec:rest:dropnamespace
  Scenario Outline: A namespace that still holds something cannot be dropped
    Given the namespace "sales" holding <content>
    When I try to drop the namespace "sales"
    Then the request is refused with 409 "NamespaceNotEmptyException"
    And the namespace "sales" exists

    Examples:
      | content                    |
      | the table "orders"         |
      | the view "recent_orders"   |
      | the child namespace "eu"   |

  @spec:rest:createnamespace
  Scenario: Creating a namespace that already exists is refused
    Given the namespace "sales" with properties:
      | key   | value    |
      | owner | data-eng |
    When I try to create the namespace "sales"
    Then the attempt fails because it already exists
    And the namespace "sales" has exactly the properties:
      | key   | value    |
      | owner | data-eng |

  @spec:rest:loadnamespacemetadata @spec:rest:listtables
  Scenario: A namespace that does not exist cannot be loaded or listed
    When I load the namespace "marketing"
    Then the request is refused with 404 "NoSuchNamespaceException"
    When I try to list the tables of "marketing"
    Then the attempt fails because the namespace does not exist
