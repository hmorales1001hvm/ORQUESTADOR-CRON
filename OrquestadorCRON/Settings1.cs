namespace OrquestadorCRON
{
    public class Settings1
    {
        public string LogPath { get; set; }
        public string PathSourceFile { get; set; }
        public string ApiRecibeTicket { get; set; }
        public string PathFileJSON { get; set; }
        public bool EsLinux { get; set; }

        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public string BucketName { get; set; }
        public string ServiceBucketURL { get; set; }

        public string RutaFacturasXML { get; set; }

        public string KushkiHost { get; set; }
        public string KushkiUserName { get; set; }
        public string KushkiPathFilePEM { get; set; }
        public string KushkiRemoteFilePath { get; set; }
        public string KushkiPathDownloadFile { get; set; }

        public string ApiURL { get; set; }

        public string AccessKeySQS { get; set; }
        public string SecretKeySQS { get; set; }
        public string RegionSQS { get; set; }
        public string QueueUrlSQS { get; set; }
    }
}
