#include "airport_telemetry.hpp"
#include <curl/curl.h>
#include <thread>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <netdb.h>
#include <arpa/inet.h>
#include <chrono>

// Static member definitions
constexpr const char *AirportTelemetrySender::PROBABILITY_HOSTNAME;
constexpr const char *AirportTelemetrySender::TARGET_URL;
// Callback function to handle response data (we don't need to store it)
size_t AirportTelemetrySender::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
  return size * nmemb; // Just discard the response
}

// Function to send the actual HTTP request
void AirportTelemetrySender::sendHTTPRequest(const std::string &json_body)
{
  CURL *curl = curl_easy_init();
  if (!curl)
  {
    return;
  }

  // Set up headers
  struct curl_slist *headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");

  // Configure curl options
  curl_easy_setopt(curl, CURLOPT_URL, TARGET_URL);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_body.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, json_body.length());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L); // 30 second timeout
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

  // Perform the request
  CURLcode res = curl_easy_perform(curl);

  if (res != CURLE_OK)
  {
    return;
  }

  // Cleanup
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
}

// Thread function that handles the entire process
void AirportTelemetrySender::requestThread(std::string json_body)
{
  sendHTTPRequest(json_body);
}

// Public function to start the request thread
void AirportTelemetrySender::sendRequestAsync(const std::string &json_body)
{
  std::thread request_thread(requestThread, json_body);
  request_thread.detach(); // Let the thread run independently
}

// Initialize curl globally (call once at program start)
void AirportTelemetrySender::initializeCurl()
{
  curl_global_init(CURL_GLOBAL_DEFAULT);
}

// Cleanup curl globally (call once at program end)
void AirportTelemetrySender::cleanupCurl()
{
  curl_global_cleanup();
}
