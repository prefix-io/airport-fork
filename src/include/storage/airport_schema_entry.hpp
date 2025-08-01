#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "storage/airport_table_set.hpp"
#include "storage/airport_scalar_function_set.hpp"
#include "storage/airport_table_function_set.hpp"

namespace duckdb
{
  class AirportTransaction;

  class AirportSchemaEntry : public SchemaCatalogEntry
  {
  public:
    AirportSchemaEntry(Catalog &catalog,
                       CreateSchemaInfo &info,
                       const string &cache_directory,
                       const AirportAPISchema &schema_data);
    ~AirportSchemaEntry() override;

  public:
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                           TableCatalogEntry &table) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
                                                   CreateTableFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
                                                  CreateCopyFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
                                                    CreatePragmaFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
    void Alter(CatalogTransaction transaction, AlterInfo &info) override;
    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    void DropEntry(ClientContext &context, DropInfo &info) override;

    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

    const AirportSerializedContentsWithSHA256Hash &serialized_source() const
    {
      return schema_data_.source();
    }

  private:
    AirportAPISchema schema_data_;

    AirportCatalogSet &GetCatalogSet(CatalogType type);
    AirportTableSet tables;
    AirportScalarFunctionSet scalar_functions;
    AirportTableFunctionSet table_functions;
  };

} // namespace duckdb
