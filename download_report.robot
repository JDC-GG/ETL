*** Settings ***
Library           OperatingSystem
Library           Collections
Library           RequestsLibrary
Library           String
Library           resources/utils.py    WITH NAME    utils

Suite Setup       Create Output Directories

*** Variables ***
${CONFIG}         config/appsettings.json
${MONITORS}       config/station_monitors.json
${DOWNLOAD_DIR}   downloads

*** Keywords ***
Create Output Directories
    Create Directory    ${DOWNLOAD_DIR}

Load JSON From File
    [Arguments]    ${path}
    ${text}=       Get File    ${path}
    ${data}=       Evaluate    __import__('json').loads($text)
    RETURN         ${data}

Build Query Params
    [Arguments]    ${station_id}    ${station_name}
    ...            ${fdate_ticks}   ${tdate_ticks}   ${tb}
    ...            ${report_type}   ${take}          ${page_size}
    ...            @{monitor_ids}

    ${mon_list}=    Create List    @{monitor_ids}
    ${tb_str}=      Convert To String    ${tb}
    @{tb_list}=     Create List          ${tb_str}

    # Llamadas a la librería Python como KEYWORDS, no como variables
    ${mon_json}=    utils.Dumps List As String    ${mon_list}
    ${tb_json}=     utils.Dumps List As String    ${tb_list}

    &{params}=      Create Dictionary
    ...             ListStationId=[${station_id}]
    ...             ListMonitorIds=${mon_json}
    ...             FDate=${fdate_ticks}
    ...             TDate=${tdate_ticks}
    ...             TB=${tb_json}
    ...             ToTB=${tb}
    ...             ReportType=${report_type}
    ...             first=true
    ...             take=${take}
    ...             skip=0
    ...             page=1
    ...             pageSize=${page_size}

    IF    '${station_name}' != ''
        @{names}=        Create List    ${station_name}
        ${names_json}=   utils.Dumps List As String    ${names}
        Set To Dictionary    ${params}    ListStationsNames=${names_json}
    END
    RETURN    ${params}

Fetch And Save One Station
    [Arguments]    ${session}   ${base_url}   ${station_id}   ${station_info}
    ...            ${fdate_ticks}   ${tdate_ticks}   ${tb}
    ...            ${report_type}   ${take}   ${page_size}   ${tz}

    ${keys}=       Get Dictionary Keys    ${station_info}
    ${name}=       Set Variable    ${EMPTY}
    IF    'name' in ${keys}
        ${name}=   Set Variable    ${station_info['name']}
    END
    ${monitors}=   Get From Dictionary    ${station_info}    monitors

    ${params}=     Build Query Params   ${station_id}   ${name}
    ...            ${fdate_ticks}   ${tdate_ticks}   ${tb}
    ...            ${report_type}   ${take}   ${page_size}
    ...            @{monitors}

    ${resp}=       Get On Session   ${session}   url=${base_url}   params=${params}   expected_status=any
    Should Be True    ${resp.status_code} >= 200 and ${resp.status_code} < 300
    ...               msg=HTTP ${resp.status_code}: ${resp.text}

    ${json_text}=  Set Variable     ${resp.text}
    ${from_iso}=   utils.ticks_to_iso   ${fdate_ticks}   ${tz}
    ${to_iso}=     utils.ticks_to_iso   ${tdate_ticks}   ${tz}

    ${safe_name}=        Replace String    ${name}    ${SPACE}    _
    ${from_iso_safe}=    Replace String    ${from_iso}    :    -
    ${from_iso_safe}=    Replace String    ${from_iso_safe}    ${SPACE}    _
    ${to_iso_safe}=      Replace String    ${to_iso}      :    -
    ${to_iso_safe}=      Replace String    ${to_iso_safe}      ${SPACE}    _

    ${fn}=         Set Variable     ${DOWNLOAD_DIR}/${station_id}_${safe_name}_${from_iso_safe}_to_${to_iso_safe}.json
    Create File    ${fn}   ${json_text}   encoding=UTF-8
    Log To Console  Saved: ${fn}

*** Test Cases ***
Download Reports For Configured Stations
    ${cfg}=        Load JSON From File    ${CONFIG}
    ${map}=        Load JSON From File    ${MONITORS}

    # Valores por defecto si el JSON no trae 'host' o 'base_url'
    ${host}=       Get From Dictionary    ${cfg}    host    http://rmcab.ambientebogota.gov.co
    ${base_url}=   Get From Dictionary    ${cfg}    base_url    /Report/GetMultiStationsReportNewAsync

    ${tz}=         Set Variable    ${cfg.get('timezone', 'America/Bogota')}
    ${stations}=   Set Variable    ${cfg['stations']}
    ${from_str}=   Set Variable    ${cfg['time']['from']}
    ${to_str}=     Set Variable    ${cfg['time']['to']}
    ${gran}=       Set Variable    ${cfg['time'].get('granularity_minutes', 60)}
    ${rtype}=      Set Variable    ${cfg['report'].get('type', 'Average')}
    ${take}=       Set Variable    ${cfg['report'].get('take', 0)}
    ${psize}=      Set Variable    ${cfg['report'].get('page_size', 0)}
    ${retry}=      Set Variable    ${cfg.get('retry', {'max_attempts': 3, 'sleep_seconds': 2})}

    ${F}=          utils.to_dotnet_ticks   ${from_str}   ${tz}
    ${T}=          utils.to_dotnet_ticks   ${to_str}     ${tz}

    # Sesión: si base_url es absoluto (empieza por http), igual funciona porque Get On Session usa la URL completa.
    Create Session   rmcab   ${host}   verify=${False}

    # Loguea cómo se está llamando (útil si algo falla)
    Log To Console    Using host: ${host}
    Log To Console    Using base_url: ${base_url}

    FOR    ${sid}    IN    @{stations}
        ${sid_str}=      Evaluate    str(${sid})
        IF    '${sid_str}' not in ${map}
            Fail    No hay monitores definidos para estación ${sid}
        END

        ${info}=         Set Variable    ${map}[${sid_str}]
        ${max}=          Set Variable    ${retry.get('max_attempts', 3)}
        ${sleep}=        Set Variable    ${retry.get('sleep_seconds', 2)}
        ${ok}=           Set Variable    ${False}

        FOR    ${i}    IN RANGE    ${max}
            TRY
                Fetch And Save One Station   rmcab   ${base_url}   ${sid}   ${info}
                ...    ${F}   ${T}   ${gran}   ${rtype}   ${take}   ${psize}   ${tz}
                ${ok}=   Set Variable   ${True}
                BREAK
            EXCEPT
                Log To Console   Intento ${i+1}/${max} falló para estación ${sid}. Reintentando en ${sleep}s...
                Sleep            ${sleep}s
            END
        END
        Should Be True   ${ok}   msg=Falla definitiva en estación ${sid}
    END
