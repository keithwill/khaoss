dotnet build -c Release -r win-x64
Set-Location "bin\Release\net7.0\win-x64\publish"
.\KHAOSS.ConsoleTest.exe
Read-Host "Finished - Press Enter to Exit"