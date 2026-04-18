@echo off
setlocal enabledelayedexpansion

:: ============================================================
:: 批量上传文件脚本 (Windows)
:: 用法: upload_files.bat <文件夹路径>
:: 示例: upload_files.bat "C:\Users\APTX-4869\Desktop\新建文件夹"
:: ============================================================

:: ---------- 可配置参数 ----------
set UPLOAD_URL=https://dataops.gt.cn/easystream_server/web/v2/sloth/depfile/add
set COOKIE=fs86_fsusername=jiangtao18; fs86_fspassword=%%7B%%22data%%22%%3A%%22JXScSWAdtXjU9ZlhhnOk1kA7XBc1uDkjbih7BxUWDoL%%2BjicyhvTF%%2BR7byv7QECiqBMpZZ1Oi60E2a%%2FrvjV%%2FLmyZhfmIoftMcKzVPvL9oEtbcCknjfc9o19ciFM9z07sBXWfmMT3GnvXaYAZPHczVzf0SI6Ygo6RDpaWUUl5PpqU%%3D%%22%%2C%%22time%%22%%3A1775308086241%%7D; bdms_uid=o2sso-74cb9af75b2b459890639549988d1256; bdms_ls=o2sso; __gsid__=cs-092c0f6019c34c7b915118308117261c; j_username=jiangtao18
set PARENT_ID=102
set DESCRIPTION=
set USER_ID=jiangtao18@gt.cn
set PRODUCT_ID=22
set PRODUCT=gt_zhty
set MAMMUT_CLUSTER=easyops-cluster
set USER_NAME=姜涛
:: --------------------------------

:: 检查参数
if "%~1"=="" (
    echo 错误: 请提供文件夹路径作为参数。
    echo 用法: %~nx0 ^<文件夹路径^>
    echo 示例: %~nx0 "C:\Users\APTX-4869\Desktop\新建文件夹"
    exit /b 1
)

set FOLDER=%~1

:: 检查文件夹是否存在
if not exist "%FOLDER%\" (
    echo 错误: 文件夹不存在: %FOLDER%
    exit /b 1
)

echo ============================================================
echo 开始批量上传文件夹中的文件:
echo   %FOLDER%
echo ============================================================
echo.

set SUCCESS_COUNT=0
set FAIL_COUNT=0

:: 遍历文件夹中的所有文件（不递归子文件夹）
for %%F in ("%FOLDER%\*") do (
    if exist "%%F" (
        if not "%%~aF"=="d" (
            echo 正在上传: %%~nxF
            curl --silent --show-error --location "%UPLOAD_URL%" ^
                --header "cookie: %COOKIE%" ^
                --form "jar=@\"%%F\"" ^
                --form "parentId=\"%PARENT_ID%\"" ^
                --form "description=\"%DESCRIPTION%\"" ^
                --form "userId=\"%USER_ID%\"" ^
                --form "productId=\"%PRODUCT_ID%\"" ^
                --form "product=\"%PRODUCT%\"" ^
                --form "mammutCluster=\"%MAMMUT_CLUSTER%\"" ^
                --form "userName=\"%USER_NAME%\""
            if !errorlevel! equ 0 (
                echo   [成功] %%~nxF
                set /a SUCCESS_COUNT+=1
            ) else (
                echo   [失败] %%~nxF ^(curl 错误码: !errorlevel!^)
                set /a FAIL_COUNT+=1
            )
            echo.
        )
    )
)

echo ============================================================
echo 上传完成: 成功 %SUCCESS_COUNT% 个, 失败 %FAIL_COUNT% 个
echo ============================================================

endlocal
