#pragma once

#include "storage/airport_catalog_set.hpp"
#include "storage/airport_table_entry.hpp"

struct AirportFunctionCatalogSchemaNameKey
{
  std::string catalog_name;
  std::string schema_name;
  std::string name;

  // Define equality operator to compare two keys
  bool operator==(const AirportFunctionCatalogSchemaNameKey &other) const
  {
    return catalog_name == other.catalog_name && schema_name == other.schema_name && name == other.name;
  }
};

namespace std
{
  template <>
  struct hash<AirportFunctionCatalogSchemaNameKey>
  {
    size_t operator()(const AirportFunctionCatalogSchemaNameKey &k) const
    {
      // Combine the hash of all 3 strings
      return hash<std::string>()(k.catalog_name) ^ (hash<std::string>()(k.schema_name) << 1) ^ (hash<std::string>()(k.name) << 2);
    }
  };
}

namespace duckdb
{
  struct CreateTableInfo;
  class AirportResult;
  class AirportSchemaEntry;
  struct AirportTableInfo;

  class AirportCatalogSetBase : public AirportInSchemaSet
  {
  protected:
    string cache_directory_;

  public:
    explicit AirportCatalogSetBase(AirportSchemaEntry &schema, const string &cache_directory)
        : AirportInSchemaSet(schema), cache_directory_(cache_directory)
    {
    }
  };

}
