#pragma once

#include "storage/airport_catalog_set.hpp"
#include "storage/airport_catalog_set_base.hpp"

namespace duckdb
{
  class AirportTableFunctionSet : public AirportCatalogSetBase
  {

  protected:
    void LoadEntries(ClientContext &context) override;

  public:
    explicit AirportTableFunctionSet(AirportCurlPool &connection_pool, AirportSchemaEntry &schema, const string &cache_directory) : AirportCatalogSetBase(connection_pool, schema, cache_directory)
    {
    }
    ~AirportTableFunctionSet() {}
  };

} // namespace duckdb
