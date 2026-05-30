using System;
using System.Reflection;
using Microsoft.Extensions.FileProviders;
class Program {
    static void Main() {
        var asm = Assembly.LoadFrom(@"c:\Users\NB-13342\RiderProjects\jobmaster-net\JobMaster.Dashboard\bin\Debug\net8.0\JobMaster.Dashboard.dll");
        var provider = new EmbeddedFileProvider(asm, "JobMaster.Dashboard.Embedded");
        Console.WriteLine("index.html exists: " + provider.GetFileInfo("index.html").Exists);
        Console.WriteLine("Embedded/index.html exists: " + provider.GetFileInfo("Embedded/index.html").Exists);
    }
}
