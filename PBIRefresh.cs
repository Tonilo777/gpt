
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.Net;
using System.IO;
using System.Text;
using System.Web.Script.Serialization;
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

                string accessToken = GetAccessToken(tenantId, clientId, clientSecret, fireAgain);

                if (string.IsNullOrEmpty(accessToken))
                    throw new Exception("Failed to obtain access token - token is null or empty");

                Dts.Variables["User::AccessToken"].Value = accessToken;
                LogInfo($"Access token obtained successfully (Length: {accessToken.Length} characters)", fireAgain);

                LogInfo("\nSTEP 2: Triggering Power BI dataset refresh...", fireAgain);
                LogInfo($"Workspace ID: {workspaceId.Substring(0, 8)}...", fireAgain);
                LogInfo($"Dataset ID: {datasetId.Substring(0, 8)}...", fireAgain);

                bool refreshTriggered = TriggerDatasetRefresh(accessToken, workspaceId, datasetId, fireAgain);

                if (refreshTriggered)
                {
                    Dts.Variables["User::RefreshStatus"].Value = "Triggered Successfully";
                    LogInfo("SUCCESS: Dataset refresh triggered successfully!", fireAgain);

                    LogInfo("Waiting 10 seconds before checking refresh status...", fireAgain);
                    Thread.Sleep(10000);

                    LogInfo("\nSTEP 3: Checking refresh status...", fireAgain);
                    string status = CheckRefreshStatus(accessToken, workspaceId, datasetId, fireAgain);

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

        private string GetAccessToken(string tenantId, string clientId, string clientSecret, bool fireAgain)
        {
            string tokenEndpoint = $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token";

            try
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

                using (WebClient client = new WebClient())
                {
                    client.Headers[HttpRequestHeader.ContentType] = "application/x-www-form-urlencoded";

                    string postData = string.Format(
                        "grant_type=client_credentials&scope=https://analysis.windows.net/powerbi/api/.default&client_id={0}&client_secret={1}",
                        Uri.EscapeDataString(clientId),
                        Uri.EscapeDataString(clientSecret));

                    LogInfo("Sending token request to Azure AD...", fireAgain);
                    string response = client.UploadString(tokenEndpoint, postData);

                    JavaScriptSerializer serializer = new JavaScriptSerializer();
                    TokenResponse tokenResponse = serializer.Deserialize<TokenResponse>(response);

                    if (tokenResponse != null && !string.IsNullOrEmpty(tokenResponse.access_token))
                    {
                        LogInfo($"Token received. Type: {tokenResponse.token_type}, Expires in: {tokenResponse.expires_in} seconds", fireAgain);
                        return tokenResponse.access_token;
                    }

                    throw new Exception("Token response was null or access_token was empty");
                }
            }
            catch (WebException webEx)
            {
                string errorResponse = "";
                if (webEx.Response != null)
                {
                    using (StreamReader reader = new StreamReader(webEx.Response.GetResponseStream()))
                    {
                        errorResponse = reader.ReadToEnd();
                    }
                }
                throw new Exception($"Failed to get access token. Status: {webEx.Status}, Response: {errorResponse}", webEx);
            }
        }

        private bool TriggerDatasetRefresh(string accessToken, string workspaceId, string datasetId, bool fireAgain)
        {
            string refreshUrl = $"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/datasets/{datasetId}/refreshes";

            try
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(refreshUrl);
                request.Method = "POST";
                request.Headers.Add("Authorization", $"Bearer {accessToken}");
                request.ContentType = "application/json";
                request.ContentLength = 0;
                request.Accept = "application/json";
                request.UserAgent = "SSIS-PowerBI-Refresh/1.0";

                LogInfo("Sending refresh request to Power BI API...", fireAgain);

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    LogInfo($"Power BI API Response: {response.StatusCode} - {response.StatusDescription}", fireAgain);
                    return response.StatusCode == HttpStatusCode.Accepted ||
                           response.StatusCode == HttpStatusCode.OK ||
                           response.StatusCode == HttpStatusCode.Created ||
                           response.StatusCode == HttpStatusCode.NoContent;
                }
            }
            catch (WebException webEx)
            {
                if (webEx.Response != null)
                {
                    HttpWebResponse errorResponse = (HttpWebResponse)webEx.Response;
                    string errorContent = "";
                    using (StreamReader reader = new StreamReader(errorResponse.GetResponseStream()))
                    {
                        errorContent = reader.ReadToEnd();
                    }

                    if ((int)errorResponse.StatusCode == 429)
                    {
                        string retryAfter = errorResponse.Headers["Retry-After"] ?? "unknown";
                        LogWarning($"Too many refresh requests. Rate limit exceeded. Retry after (seconds): {retryAfter}");
                        return false;
                    }

                    if (errorResponse.StatusCode == HttpStatusCode.Unauthorized)
                        throw new Exception("Unauthorized access. Check token and permissions.");

                    if (errorResponse.StatusCode == HttpStatusCode.NotFound)
                        throw new Exception($"Dataset or workspace not found. Workspace: {workspaceId}, Dataset: {datasetId}");

                    throw new Exception($"Failed to trigger refresh. Status: {errorResponse.StatusCode}, Error: {errorContent}");
                }

                throw new Exception($"Failed to trigger refresh: {webEx.Message}", webEx);
            }
        }

        private string CheckRefreshStatus(string accessToken, string workspaceId, string datasetId, bool fireAgain)
        {
            string statusUrl = $"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/datasets/{datasetId}/refreshes?$top=1";

            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(statusUrl);
                request.Method = "GET";
                request.Headers.Add("Authorization", $"Bearer {accessToken}");
                request.Accept = "application/json";
                request.UserAgent = "SSIS-PowerBI-Refresh/1.0";

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                {
                    string responseText = reader.ReadToEnd();

                    JavaScriptSerializer serializer = new JavaScriptSerializer();
                    RefreshResponse refreshResponse = serializer.Deserialize<RefreshResponse>(responseText);

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