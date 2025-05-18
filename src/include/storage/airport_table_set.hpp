#pragma once

#include "storage/airport_catalog_set.hpp"
#include "storage/airport_table_entry.hpp"

#include "storage/airport_catalog_set_base.hpp"

namespace duckdb
{
  struct CreateTableInfo;
  class AirportResult;
  class AirportSchemaEntry;
  class AirportCurlPool;
  struct AirportTableInfo;

  class AirportTableSet : public AirportCatalogSetBase
  {
  public:
    explicit AirportTableSet(AirportCurlPool &connection_pool, AirportSchemaEntry &schema, const string &cache_directory) : AirportCatalogSetBase(connection_pool, schema, cache_directory)
    {
    }
    ~AirportTableSet() {}

  public:
    optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup_info) override;

    optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

    static unique_ptr<AirportTableInfo> GetTableInfo(ClientContext &context, AirportSchemaEntry &schema,
                                                     const string &table_name);
    optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

    void AlterTable(ClientContext &context, AlterTableInfo &info);

  protected:
    void LoadEntries(ClientContext &context) override;

  private:
    vector<unique_ptr<CatalogEntry>> point_in_time_entries_;
  };

  class AirportTableEntry;

  unique_ptr<AirportTableEntry> AirportCatalogEntryFromFlightInfo(
      std::unique_ptr<arrow::flight::FlightInfo> flight_info,
      const std::string &server_location,
      SchemaCatalogEntry &schema_entry,
      Catalog &catalog,
      ClientContext &context);

} // namespace duckdb
