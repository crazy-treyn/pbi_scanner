#include "auth.hpp"

#include "connection_string.hpp"
#include "http_client.hpp"
#include "pbi_scanner_util.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "yyjson.hpp"

#include <cstdio>
#include <cstdlib>
#include <algorithm>

#ifdef _WIN32
#define PBI_XMLA_POPEN _popen
#define PBI_XMLA_PCLOSE _pclose
#else
#define PBI_XMLA_POPEN popen
#define PBI_XMLA_PCLOSE pclose
#endif

namespace duckdb {
using namespace duckdb_yyjson; // NOLINT

namespace {

static constexpr const char *POWER_BI_SCOPE =
    "https://analysis.windows.net/powerbi/api/.default";
static constexpr const char *SUPPORTED_DEFAULT_CHAIN[] = {"env", "cli"};

struct ServicePrincipalCredentials {
  string tenant_id;
  string client_id;
  string client_secret;

  bool IsComplete() const {
    return !tenant_id.empty() && !client_id.empty() && !client_secret.empty();
  }
};

static string
GetOptionalNamedParameter(const named_parameter_map_t &named_parameters,
                          const string &name) {
  auto entry = named_parameters.find(name);
  if (entry == named_parameters.end() || entry->second.IsNull()) {
    return string();
  }
  return Trimmed(entry->second.ToString());
}

static bool
HasNonNullNamedParameter(const named_parameter_map_t &named_parameters,
                         const string &name) {
  auto entry = named_parameters.find(name);
  return entry != named_parameters.end() && !entry->second.IsNull();
}

static string GetOptionalEnv(const char *name) {
  auto *value = std::getenv(name);
  if (!value || !*value) {
    return string();
  }
  return Trimmed(value);
}

static string UrlEncode(const string &value) {
  static const char *hex = "0123456789ABCDEF";
  string encoded;
  encoded.reserve(value.size() * 3);
  for (auto ch : value) {
    auto uch = static_cast<unsigned char>(ch);
    if ((uch >= 'A' && uch <= 'Z') || (uch >= 'a' && uch <= 'z') ||
        (uch >= '0' && uch <= '9') || uch == '-' || uch == '_' || uch == '.' ||
        uch == '~') {
      encoded.push_back(static_cast<char>(uch));
    } else {
      encoded.push_back('%');
      encoded.push_back(hex[(uch >> 4) & 0xF]);
      encoded.push_back(hex[uch & 0xF]);
    }
  }
  return encoded;
}

static string GetOptionalSecretValue(const KeyValueSecret &secret,
                                     const string &key) {
  Value value;
  if (!secret.TryGetValue(key, value) || value.IsNull()) {
    return string();
  }
  return Trimmed(value.ToString());
}

static bool HasDirectAuthInputs(const named_parameter_map_t &named_parameters) {
  return HasNonNullNamedParameter(named_parameters, "auth_mode") ||
         HasNonNullNamedParameter(named_parameters, "access_token") ||
         HasNonNullNamedParameter(named_parameters, "tenant_id") ||
         HasNonNullNamedParameter(named_parameters, "client_id") ||
         HasNonNullNamedParameter(named_parameters, "client_secret");
}

static string
ResolveSecretReference(const PowerBIConnectionConfig &connection_config,
                       const named_parameter_map_t &named_parameters) {
  auto secret_from_parameter =
      GetOptionalNamedParameter(named_parameters, "secret_name");
  auto parameter_was_provided =
      HasNonNullNamedParameter(named_parameters, "secret_name");
  if (parameter_was_provided && secret_from_parameter.empty()) {
    throw InvalidInputException("secret_name must not be empty");
  }

  auto secret_from_connection_string = Trimmed(connection_config.secret_name);
  if (!secret_from_connection_string.empty() &&
      !secret_from_parameter.empty() &&
      !StringUtil::CIEquals(secret_from_connection_string,
                            secret_from_parameter)) {
    throw InvalidInputException("secret_name named parameter conflicts with "
                                "Secret in the connection string");
  }
  if (!secret_from_parameter.empty()) {
    return secret_from_parameter;
  }
  return secret_from_connection_string;
}

static string
ResolveAccessTokenMode(const named_parameter_map_t &named_parameters) {
  auto explicit_token =
      GetOptionalNamedParameter(named_parameters, "access_token");
  if (!explicit_token.empty()) {
    return explicit_token;
  }
  auto env_token = GetOptionalEnv("PBI_XMLA_ACCESS_TOKEN");
  if (!env_token.empty()) {
    return env_token;
  }
  throw InvalidInputException(
      "access_token named parameter or PBI_XMLA_ACCESS_TOKEN environment "
      "variable is required");
}

static string ResolveAzureCliMode() {
  string command = string("az account get-access-token --scope ") +
                   POWER_BI_SCOPE + " --query accessToken -o tsv 2>/dev/null";
  auto *pipe = PBI_XMLA_POPEN(command.c_str(), "r");
  if (!pipe) {
    throw IOException("failed to invoke Azure CLI");
  }

  string output;
  char buffer[512];
  while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    output += buffer;
  }
  auto exit_code = PBI_XMLA_PCLOSE(pipe);
  auto token = Trimmed(output);
  if (exit_code != 0 || token.empty()) {
    throw IOException("Azure CLI token acquisition failed; run az login with "
                      "the Power BI scope first");
  }
  return token;
}

static ServicePrincipalCredentials
ResolveServicePrincipalCredentialsFromEnv(bool allow_legacy_fallback) {
  ServicePrincipalCredentials credentials;
  credentials.tenant_id = GetOptionalEnv("AZURE_TENANT_ID");
  credentials.client_id = GetOptionalEnv("AZURE_CLIENT_ID");
  credentials.client_secret = GetOptionalEnv("AZURE_CLIENT_SECRET");
  if (credentials.IsComplete() || !allow_legacy_fallback) {
    return credentials;
  }

  if (credentials.tenant_id.empty()) {
    credentials.tenant_id = GetOptionalEnv("PBI_XMLA_TENANT_ID");
  }
  if (credentials.client_id.empty()) {
    credentials.client_id = GetOptionalEnv("PBI_XMLA_CLIENT_ID");
  }
  if (credentials.client_secret.empty()) {
    credentials.client_secret = GetOptionalEnv("PBI_XMLA_CLIENT_SECRET");
  }
  return credentials;
}

static ServicePrincipalCredentials ResolveServicePrincipalCredentials(
    const named_parameter_map_t &named_parameters,
    bool allow_legacy_env_fallback) {
  auto env_credentials =
      ResolveServicePrincipalCredentialsFromEnv(allow_legacy_env_fallback);

  ServicePrincipalCredentials credentials;
  credentials.tenant_id =
      GetOptionalNamedParameter(named_parameters, "tenant_id");
  credentials.client_id =
      GetOptionalNamedParameter(named_parameters, "client_id");
  credentials.client_secret =
      GetOptionalNamedParameter(named_parameters, "client_secret");

  if (credentials.tenant_id.empty()) {
    credentials.tenant_id = env_credentials.tenant_id;
  }
  if (credentials.client_id.empty()) {
    credentials.client_id = env_credentials.client_id;
  }
  if (credentials.client_secret.empty()) {
    credentials.client_secret = env_credentials.client_secret;
  }
  return credentials;
}

static std::unique_ptr<yyjson_doc, void (*)(yyjson_doc *)>
ReadJsonDocument(const HttpResponse &response, const string &label) {
  if (response.HasRequestError()) {
    throw IOException("%s request failed: %s", label, response.request_error);
  }
  if (response.status >= 400) {
    throw IOException("%s request http %d: %s", label, response.status,
                      response.body);
  }

  auto *document = yyjson_read(response.body.c_str(), response.body.size(),
                               YYJSON_READ_NOFLAG);
  if (!document) {
    throw IOException("%s response was not valid JSON", label);
  }
  return std::unique_ptr<yyjson_doc, void (*)(yyjson_doc *)>(document,
                                                             yyjson_doc_free);
}

static string GetRequiredJSONString(yyjson_val *object, const char *key,
                                    const string &label) {
  auto *value = yyjson_obj_get(object, key);
  if (!value || !yyjson_is_str(value) || !yyjson_get_str(value)) {
    throw IOException("%s response did not include %s", label, key);
  }
  return yyjson_get_str(value);
}

static string
AcquireServicePrincipalToken(const ServicePrincipalCredentials &credentials) {
  HttpClient client(300000);
  auto url = string("https://login.microsoftonline.com/") +
             credentials.tenant_id + "/oauth2/v2.0/token";
  auto body = string("grant_type=client_credentials&client_id=") +
              UrlEncode(credentials.client_id) +
              "&client_secret=" + UrlEncode(credentials.client_secret) +
              "&scope=" + UrlEncode(POWER_BI_SCOPE);
  auto response =
      client.Post(url, {}, body, "application/x-www-form-urlencoded");
  auto doc_holder = ReadJsonDocument(response, "service principal token");
  auto *root = yyjson_doc_get_root(doc_holder.get());
  if (!root || !yyjson_is_obj(root)) {
    throw IOException("service principal token response was not a JSON object");
  }
  return GetRequiredJSONString(root, "access_token", "service principal token");
}

static string
ResolveServicePrincipalMode(const named_parameter_map_t &named_parameters) {
  auto credentials = ResolveServicePrincipalCredentials(named_parameters, true);
  if (!credentials.IsComplete()) {
    throw InvalidInputException(
        "service_principal auth requires tenant_id, client_id, and "
        "client_secret or standard Azure environment variables");
  }
  return AcquireServicePrincipalToken(credentials);
}

static string
ResolveDirectAuthMode(const named_parameter_map_t &named_parameters) {
  auto auth_mode = StringUtil::Lower(
      GetOptionalNamedParameter(named_parameters, "auth_mode"));
  if (!auth_mode.empty()) {
    return auth_mode;
  }
  if (HasNonNullNamedParameter(named_parameters, "access_token")) {
    return "access_token";
  }
  if (HasNonNullNamedParameter(named_parameters, "tenant_id") ||
      HasNonNullNamedParameter(named_parameters, "client_id") ||
      HasNonNullNamedParameter(named_parameters, "client_secret")) {
    return "service_principal";
  }
  return string();
}

static string
ResolveDirectAccessToken(const named_parameter_map_t &named_parameters) {
  auto auth_mode = ResolveDirectAuthMode(named_parameters);
  if (auth_mode.empty() || auth_mode == "access_token") {
    return ResolveAccessTokenMode(named_parameters);
  }
  if (auth_mode == "azure_cli" || auth_mode == "cli") {
    return ResolveAzureCliMode();
  }
  if (auth_mode == "service_principal") {
    return ResolveServicePrincipalMode(named_parameters);
  }
  throw InvalidInputException("unsupported auth_mode \"%s\"", auth_mode);
}

static vector<string> ParseChain(const string &chain) {
  vector<string> result;
  auto members =
      chain.empty() ? vector<string>{"default"} : StringUtil::Split(chain, ';');
  for (const auto &member : members) {
    auto normalized_member = StringUtil::Lower(Trimmed(member));
    if (normalized_member.empty()) {
      continue;
    }
    if (normalized_member == "default") {
      for (const auto *default_member : SUPPORTED_DEFAULT_CHAIN) {
        if (std::find(result.begin(), result.end(), default_member) ==
            result.end()) {
          result.emplace_back(default_member);
        }
      }
      continue;
    }
    if (std::find(result.begin(), result.end(), normalized_member) ==
        result.end()) {
      result.push_back(normalized_member);
    }
  }
  if (result.empty()) {
    for (const auto *default_member : SUPPORTED_DEFAULT_CHAIN) {
      result.emplace_back(default_member);
    }
  }
  return result;
}

static string ResolveCredentialChainSecret(const KeyValueSecret &secret,
                                           const string &secret_name) {
  auto chain = ParseChain(GetOptionalSecretValue(secret, "chain"));
  vector<string> failures;
  vector<string> unsupported;

  for (const auto &provider : chain) {
    try {
      if (provider == "cli") {
        return ResolveAzureCliMode();
      }
      if (provider == "env") {
        auto credentials = ResolveServicePrincipalCredentialsFromEnv(true);
        if (!credentials.IsComplete()) {
          throw InvalidInputException(
              "standard Azure environment variables AZURE_TENANT_ID, "
              "AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET are required");
        }
        return AcquireServicePrincipalToken(credentials);
      }
      if (provider == "managed_identity" || provider == "workload_identity") {
        unsupported.push_back(provider);
        continue;
      }
      unsupported.push_back(provider);
    } catch (const Exception &ex) {
      failures.push_back(provider + ": " + string(ex.what()));
    } catch (const std::exception &ex) {
      failures.push_back(provider + ": " + string(ex.what()));
    }
  }

  string message =
      "Secret \"" + secret_name + "\" did not yield a Power BI token";
  if (!unsupported.empty()) {
    message +=
        "; unsupported chain providers: " + StringUtil::Join(unsupported, ", ");
  }
  if (!failures.empty()) {
    message += "; attempts failed: " + StringUtil::Join(failures, " | ");
  }
  throw IOException("%s", message);
}

static string ResolveServicePrincipalSecret(const KeyValueSecret &secret,
                                            const string &secret_name) {
  ServicePrincipalCredentials credentials;
  credentials.tenant_id = GetOptionalSecretValue(secret, "tenant_id");
  credentials.client_id = GetOptionalSecretValue(secret, "client_id");
  credentials.client_secret = GetOptionalSecretValue(secret, "client_secret");

  auto client_certificate_path =
      GetOptionalSecretValue(secret, "client_certificate_path");
  if (credentials.client_secret.empty() && !client_certificate_path.empty()) {
    throw InvalidInputException("Secret \"%s\" uses client_certificate_path, "
                                "which is not supported yet",
                                secret_name);
  }
  if (!credentials.IsComplete()) {
    throw InvalidInputException(
        "Secret \"%s\" must include tenant_id, client_id, and client_secret",
        secret_name);
  }
  return AcquireServicePrincipalToken(credentials);
}

static string ResolveSecretBackedAccessToken(ClientContext &context,
                                             const string &secret_name) {
  auto &secret_manager = SecretManager::Get(context);
  auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
  auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
  if (!secret_entry || !secret_entry->secret) {
    throw InvalidInputException("Secret \"%s\" was not found", secret_name);
  }

  auto &base_secret = *secret_entry->secret;
  if (!StringUtil::CIEquals(base_secret.GetType(), "azure")) {
    throw InvalidInputException("Secret \"%s\" must be TYPE azure",
                                secret_name);
  }

  auto *key_value_secret =
      dynamic_cast<const KeyValueSecret *>(secret_entry->secret.get());
  if (!key_value_secret) {
    throw InvalidInputException("Secret \"%s\" is not a key-value secret",
                                secret_name);
  }

  auto provider = StringUtil::Lower(Trimmed(base_secret.GetProvider()));
  if (provider == "credential_chain") {
    return ResolveCredentialChainSecret(*key_value_secret, secret_name);
  }
  if (provider == "service_principal") {
    return ResolveServicePrincipalSecret(*key_value_secret, secret_name);
  }
  if (provider == "config") {
    throw InvalidInputException("Secret \"%s\" uses azure provider config, "
                                "which is not supported for Power BI auth",
                                secret_name);
  }
  if (provider == "managed_identity" || provider == "workload_identity") {
    throw InvalidInputException(
        "Secret \"%s\" uses azure provider %s, which is not implemented yet",
        secret_name, provider);
  }
  throw InvalidInputException(
      "Secret \"%s\" uses unsupported azure provider %s", secret_name,
      provider);
}

} // namespace

string
ResolvePowerBIAccessToken(ClientContext &context,
                          const PowerBIConnectionConfig &connection_config,
                          const named_parameter_map_t &named_parameters) {
  if (HasDirectAuthInputs(named_parameters)) {
    return ResolveDirectAccessToken(named_parameters);
  }

  auto secret_name =
      ResolveSecretReference(connection_config, named_parameters);
  if (!secret_name.empty()) {
    return ResolveSecretBackedAccessToken(context, secret_name);
  }

  return ResolveDirectAccessToken(named_parameters);
}

static string ExtractExceptionMessage(const string &error_text) {
  auto *document =
      yyjson_read(error_text.c_str(), error_text.size(), YYJSON_READ_NOFLAG);
  if (!document) {
    return error_text;
  }
  std::unique_ptr<yyjson_doc, void (*)(yyjson_doc *)> doc_holder(
      document, yyjson_doc_free);
  auto *root = yyjson_doc_get_root(document);
  if (!root || !yyjson_is_obj(root)) {
    return error_text;
  }
  auto *message = yyjson_obj_get(root, "exception_message");
  if (!message || !yyjson_is_str(message) || !yyjson_get_str(message)) {
    return error_text;
  }
  return yyjson_get_str(message);
}

string TestServicePrincipalAuthErrorMessage(const string &test_case) {
  HttpResponse response;
  if (test_case == "request_error") {
    response.request_error = "simulated_request_error";
  } else if (test_case == "http_error") {
    response.status = 401;
    response.body = "denied";
  } else if (test_case == "invalid_json") {
    response.status = 200;
    response.body = "{not-json}";
  } else if (test_case == "missing_access_token") {
    response.status = 200;
    response.body = R"({"token_type":"Bearer"})";
  } else {
    throw InvalidInputException("unknown auth test case \"%s\"", test_case);
  }

  try {
    auto document = ReadJsonDocument(response, "service principal token");
    auto *root = yyjson_doc_get_root(document.get());
    if (!root || !yyjson_is_obj(root)) {
      throw IOException(
          "service principal token response was not a JSON object");
    }
    GetRequiredJSONString(root, "access_token", "service principal token");
    return string();
  } catch (const Exception &ex) {
    return ExtractExceptionMessage(ex.what());
  } catch (const std::exception &ex) {
    return ExtractExceptionMessage(ex.what());
  }
}

} // namespace duckdb
