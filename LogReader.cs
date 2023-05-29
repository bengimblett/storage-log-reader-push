using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Http;
using Azure.Core;
using Azure.Identity;

namespace poc;

public class LogReader{

 

    /// obtain a lease - status tag isnt important here because the lease should be exclusive
    private async Task<BlobLeaseClient> GetLeaseOnBlob(BlobClient blobClient, int durationSeconds)
    {
        BlobLeaseClient leaseClient = blobClient.GetBlobLeaseClient();

        // this is exclusive r/w
        await leaseClient.AcquireAsync(TimeSpan.FromSeconds(durationSeconds));

        return leaseClient;
    }

    /// release the lease and mark the blob as read/processed 
    private async Task ReleaseAndMarkAsCompleted(BlobClient blobClient, BlobLeaseClient leaseClient)
    {
        // After the file has been processed, a `Status` tag with a value of "Complete" is added to the blob using the `SetTagsAsync` method.
        await blobClient.SetTagsAsync(new Dictionary<string, string>() { { "Status", "Processed" } });
        await leaseClient.ReleaseAsync();
    }

    private static async Task<MemoryStream> DownloadBlobToStreamAsync(BlobClient blobClient)
    {
        var ms = new MemoryStream();

        await blobClient.DownloadToAsync(ms);

        // rewind
        ms.Seek(0,SeekOrigin.Begin);

        return ms;
    }


 
    /// This method looks at what to do with the blob acquired from a filtered list upstream 
    /// This must happen on r/w  exclusive lease
    ///
    /// Question: Does application of a tag change the etag? No.
    public async Task<bool> ProcessBlob(BlobClient blobClient)
    {
        if ( null == blobClient){
            throw new ArgumentNullException("blobClient");
        }
        if ( !await blobClient.ExistsAsync()){
            throw new ArgumentException("blobClient");
        }
        BlobLeaseClient leaseClient;
        try
        {
            // try and get a lease, quit if not possible (assumes competing consumer)
            //TODO: Lease duration should be a config property 
            leaseClient = await GetLeaseOnBlob(blobClient, 60);

        }
        catch (Exception e)
        {
            // TODO: Although we expect a lease/lock could fail and another thread/process will try again
            // should still log the error in case something else went wrong
             Console.WriteLine(e.Message);
            return false;
        }
        bool logWritten=false;
        MemoryStream blobStream;
        try{
            // TODO: consider the need to buffer how big are the blobs going to be?
            blobStream =await DownloadBlobToStreamAsync(blobClient);
        }
        catch (Exception e){
            // TODO: log
            // quit
            Console.WriteLine(e.Message);
            return false;
        }
        
        try{
            logWritten=await new LogWriter().WriteLog(blobStream);
            if (logWritten ){
                await ReleaseAndMarkAsCompleted(blobClient, leaseClient);
            }
        }
        catch (Exception e){
            //  TODO: compensation may be required if the log got written but something went wrang with either the lease or the status tag update
             Console.WriteLine(e.Message);
        }
        return logWritten;
    }

}