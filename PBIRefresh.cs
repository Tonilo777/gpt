using System;
using Microsoft.SqlServer.Dts.Runtime;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Threading;

namespace ST_PowerBIRefresh
{
    [Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
    public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
    {
        public class TokenResponse
        {
            public string access_token { get; set; }
            public string token_type { get; set; }
            public int expires_in { get; set; }
        }

        public class RefreshValue
        {
            public string id { get; set; }
            public string refreshType { get; set; }
            public DateTime startTime { get; set; }
            public DateTime endTime { get; set; }
            public string status { get; set; }
            public string serviceExceptionJson { get; set; }
        }

        public class RefreshResponse
        {
            public List<RefreshValue> value { get; set; }
        }
        private static readonly HttpClient HttpClient = new HttpClient();
        private const string TokenUrlFormat = "https://login.microsoftonline.com/{0}/oauth2/v2.0/token";
        private const string RefreshUrlFormat = "https://api.powerbi.com/v1.0/myorg/groups/{0}/datasets/{1}/refreshes";
        private const string StatusUrlFormat = "https://api.powerbi.com/v1.0/myorg/groups/{0}/datasets/{1}/refreshes?$top=1";
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        public void Main()
        {
            bool fireAgain = true;

            try
            {
                LogInfo("Power BI Refresh Process Started", fireAgain);
                LogInfo("=================================", fireAgain);

                string tenantId = Dts.Variables["User::TenantId"].Value.ToString();
                string clientId = Dts.Variables["User::ClientId"].Value.ToString();
                string clientSecret = Dts.Variables["User::ClientSecret"].Value.ToString();
                string workspaceId = Dts.Variables["User::WorkspaceId"].Value.ToString();
                string datasetId = Dts.Variables["User::DatasetId"].Value.ToString();

                LogInfo("Validating input parameters...", fireAgain);
                ValidateInputs(tenantId, clientId, clientSecret, workspaceId, datasetId);
                LogInfo("Input validation successful", fireAgain);

                LogInfo("\nSTEP 1: Obtaining access token from Azure AD...", fireAgain);
                LogInfo($"Tenant ID: {tenantId.Substring(0, 8)}...", fireAgain);
                LogInfo($"Client ID: {clientId.Substring(0, 8)}...", fireAgain);

                string accessToken = GetAccessTokenAsync(tenantId, clientId, clientSecret, fireAgain).GetAwaiter().GetResult();

                if (string.IsNullOrEmpty(accessToken))
                    throw new Exception("Failed to obtain access token - token is null or empty");

                Dts.Variables["User::AccessToken"].Value = accessToken;
                LogInfo($"Access token obtained successfully (Length: {accessToken.Length} characters)", fireAgain);

                LogInfo("\nSTEP 2: Triggering Power BI dataset refresh...", fireAgain);
                LogInfo($"Workspace ID: {workspaceId.Substring(0, 8)}...", fireAgain);
                LogInfo($"Dataset ID: {datasetId.Substring(0, 8)}...", fireAgain);

                bool refreshTriggered = TriggerDatasetRefreshAsync(accessToken, workspaceId, datasetId, fireAgain).GetAwaiter().GetResult();

                if (refreshTriggered)
                {
                    Dts.Variables["User::RefreshStatus"].Value = "Triggered Successfully";
                    LogInfo("SUCCESS: Dataset refresh triggered successfully!", fireAgain);

                    LogInfo("Waiting 10 seconds before checking refresh status...", fireAgain);
                    Thread.Sleep(10000);

                    LogInfo("\nSTEP 3: Checking refresh status...", fireAgain);
                    string status = CheckRefreshStatusAsync(accessToken, workspaceId, datasetId, fireAgain).GetAwaiter().GetResult();

                    if (!string.IsNullOrEmpty(status))
                    {
                        Dts.Variables["User::RefreshStatus"].Value = status;
                        LogInfo($"Current refresh status: {status}", fireAgain);
                    }
                }
                else
                {
                    throw new Exception("Failed to trigger dataset refresh");
                }

                LogInfo("\n=================================", fireAgain);
                LogInfo("Power BI Refresh Process Completed Successfully", fireAgain);

                Dts.TaskResult = (int)ScriptResults.Success;
            }
            catch (Exception ex)
            {
                LogError($"Power BI Refresh Failed: {ex.Message}");
                LogError($"Stack Trace: {ex.StackTrace}");

                if (ex.InnerException != null)
                    LogError($"Inner Exception: {ex.InnerException.Message}");

                Dts.TaskResult = (int)ScriptResults.Failure;
            }
        }

        private void ValidateInputs(string tenantId, string clientId, string clientSecret, string workspaceId, string datasetId)
        {
            if (string.IsNullOrWhiteSpace(tenantId) || !Guid.TryParse(tenantId, out _))
                throw new ArgumentException("Tenant ID is invalid");
            if (string.IsNullOrWhiteSpace(clientId) || !Guid.TryParse(clientId, out _))
                throw new ArgumentException("Client ID is invalid");
            if (string.IsNullOrWhiteSpace(clientSecret))
                throw new ArgumentException("Client Secret is required");
            if (string.IsNullOrWhiteSpace(workspaceId) || !Guid.TryParse(workspaceId, out _))
                throw new ArgumentException("Workspace ID is invalid");
            if (string.IsNullOrWhiteSpace(datasetId) || !Guid.TryParse(datasetId, out _))
                throw new ArgumentException("Dataset ID is invalid");
        }

        private async Task<string> GetAccessTokenAsync(string tenantId, string clientId, string clientSecret, bool fireAgain)
        {
            string tokenEndpoint = string.Format(TokenUrlFormat, tenantId);

            try
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

                var content = new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    {"grant_type", "client_credentials"},
                    {"scope", "https://analysis.windows.net/powerbi/api/.default"},
                    {"client_id", clientId},
                    {"client_secret", clientSecret}
                });

                LogInfo("Sending token request to Azure AD...", fireAgain);
                using HttpResponseMessage response = await HttpClient.PostAsync(tokenEndpoint, content);
                string responseText = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                    throw new Exception($"Failed to get access token. Status: {response.StatusCode}, Response: {responseText}");

                TokenResponse tokenResponse = JsonSerializer.Deserialize<TokenResponse>(responseText, JsonOptions);

                if (tokenResponse != null && !string.IsNullOrEmpty(tokenResponse.access_token))
                {
                    LogInfo($"Token received. Type: {tokenResponse.token_type}, Expires in: {tokenResponse.expires_in} seconds", fireAgain);
                    return tokenResponse.access_token;
                }

                throw new Exception("Token response was null or access_token was empty");
            }
            catch (HttpRequestException httpEx)
            {
                throw new Exception($"Failed to get access token: {httpEx.Message}", httpEx);
            }
        }

        private async Task<bool> TriggerDatasetRefreshAsync(string accessToken, string workspaceId, string datasetId, bool fireAgain)
        {
            string refreshUrl = string.Format(RefreshUrlFormat, workspaceId, datasetId);

            try
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                using var request = new HttpRequestMessage(HttpMethod.Post, refreshUrl);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                request.Content = new StringContent(string.Empty, Encoding.UTF8, "application/json");
                request.Headers.UserAgent.ParseAdd("SSIS-PowerBI-Refresh/1.0");

                LogInfo("Sending refresh request to Power BI API...", fireAgain);

                using HttpResponseMessage response = await HttpClient.SendAsync(request);
                string responseText = await response.Content.ReadAsStringAsync();

                LogInfo($"Power BI API Response: {response.StatusCode} - {response.ReasonPhrase}", fireAgain);

                if (response.IsSuccessStatusCode || response.StatusCode == HttpStatusCode.Accepted)
                    return true;

                if ((int)response.StatusCode == 429)
                {
                    string retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds.ToString() ?? "unknown";
                    LogWarning($"Too many refresh requests. Rate limit exceeded. Retry after (seconds): {retryAfter}");
                    return false;
                }

                if (response.StatusCode == HttpStatusCode.Unauthorized)
                    throw new Exception("Unauthorized access. Check token and permissions.");

                if (response.StatusCode == HttpStatusCode.NotFound)
                    throw new Exception($"Dataset or workspace not found. Workspace: {workspaceId}, Dataset: {datasetId}");

                throw new Exception($"Failed to trigger refresh. Status: {response.StatusCode}, Error: {responseText}");
            }
            catch (HttpRequestException webEx)
            {
                throw new Exception($"Failed to trigger refresh: {webEx.Message}", webEx);
            }
        }

        private async Task<string> CheckRefreshStatusAsync(string accessToken, string workspaceId, string datasetId, bool fireAgain)
        {
            string statusUrl = string.Format(StatusUrlFormat, workspaceId, datasetId);

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, statusUrl);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                request.Headers.UserAgent.ParseAdd("SSIS-PowerBI-Refresh/1.0");

                using HttpResponseMessage response = await HttpClient.SendAsync(request);
                string responseText = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                    throw new Exception($"Failed to check refresh status. Status: {response.StatusCode}, Error: {responseText}");

                RefreshResponse refreshResponse = JsonSerializer.Deserialize<RefreshResponse>(responseText, JsonOptions);

                if (refreshResponse?.value?.Count > 0)
                {
                    var latestRefresh = refreshResponse.value[0];
                    LogInfo($"Refresh ID: {latestRefresh.id}", fireAgain);
                    LogInfo($"Refresh Type: {latestRefresh.refreshType}", fireAgain);
                    LogInfo($"Start Time: {latestRefresh.startTime}", fireAgain);
                    LogInfo($"Status: {latestRefresh.status}", fireAgain);

                    if (!string.IsNullOrEmpty(latestRefresh.serviceExceptionJson))
                        LogWarning($"Service Exception: {latestRefresh.serviceExceptionJson}");

                    return latestRefresh.status;
                }

                LogWarning("No refresh history found for this dataset");
                return "Unknown";
            }
            catch (Exception ex)
            {
                LogWarning($"Could not check refresh status: {ex.Message}");
                return null;
            }
        }

        private void LogInfo(string message, bool fireAgain)
        {
            Dts.Events.FireInformation(0, "Power BI Refresh", message, "", 0, ref fireAgain);
        }

        private void LogWarning(string message)
        {
            bool fireAgain = true;
            Dts.Events.FireWarning(0, "Power BI Refresh", message, "", 0);
        }

        private void LogError(string message)
        {
            Dts.Events.FireError(0, "Power BI Refresh", message, "", 0);
        }

        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        }
    }
}