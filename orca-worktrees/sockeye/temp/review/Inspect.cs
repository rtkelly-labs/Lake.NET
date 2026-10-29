using System.Reflection;
var a = Assembly.LoadFrom(args[0]);
foreach (var c in a.GetType("Amazon.Runtime.AmazonUnmarshallingException")!.GetConstructors()) Console.WriteLine(c);
