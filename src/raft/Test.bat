del /q E:\git-workspace\6.824\TestResault\*.*
for /L %%i in (1,1,10) DO (
    go test -run Figure8Unreliable2C -race >> E:\git-workspace\6.824\TestResault\test%%i.txt
)