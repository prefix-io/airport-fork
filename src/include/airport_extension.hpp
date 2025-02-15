#pragma once

#include "duckdb.hpp"
#include <arrow/util/windows_compatibility.h>

namespace duckdb
{

	class AirportExtension : public Extension
	{
	public:
		void Load(DuckDB &db) override;
		std::string Name() override;
		std::string Version() const override;
	};

	void AddListFlightsFunction(DatabaseInstance &instance);

	void AddTakeFlightFunction(DatabaseInstance &instance);

} // namespace duckdb
