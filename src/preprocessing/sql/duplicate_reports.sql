SELECT
    -- ID and Group Variables
    base_table.Report_Group_Flag,
    base_table.ReportID,
    [Vendor Number] AS Vendor_Number,
    [Document Type] AS Document_Type,

-- Invoice Number Variables
    len([Invoice Number]) AS Invoice_Number_Length,
    try_cast([Invoice Number] AS BIGINT) AS Invoice_Number_Numeric,
    len(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 256)) /
    (len([Invoice Number]) * 1.0) AS Invoice_Number_AlphasPct,
    1 - (len(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 256)) /
         (len([Invoice Number]) * 1.0)) AS Invoice_Number_NumsPct,
    IIF(len(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 256)) !=
        len(try_cast(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 256) AS BIGINT)),
        len(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 256)) -
        len(try_cast(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 256) AS BIGINT)),
        NULL) AS Invoice_Number_LeadingZeroes,

-- Daily Invoice Count
    COUNT([Invoice Date]) OVER ( PARTITION BY base_table.Report_Group_Flag, base_table.ReportID) AS Daily_Invoice_Count,

-- Date Variables
    datediff(D, [Invoice Date], [Check Date]) AS Days_Invoice_to_Check_Date,
    datediff(D, [Invoice Date], [Clearing Date]) AS Days_Invoice_to_Clearing_Date,
    datediff(D, [Invoice Date], [Posting Date]) AS Days_Invoice_to_Posting_Date,
    datediff(D, [Invoice Date], [Void Date]) AS Days_Invoice_to_Void_Date,


-- Amount Variables
    [Gross Invoice Amount] AS Gross_Invoice_Amount,
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%1%', 1, 0) +
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%2%', 1, 0) +
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%3%', 1, 0) +
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%4%', 1, 0) +
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%5%', 1, 0) +
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%6%', 1, 0) +
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%7%', 1, 0) +
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%8%', 1, 0) +
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%9%', 1, 0) +
    IIF(cast([Gross Invoice Amount] AS NVARCHAR) LIKE '%0%', 1, 0) AS UniqueDigits_Gross_Invoice_Amount,

    IIF([Gross Invoice Amount] % 1 > 0, 1, 0) AS HasFrac_Gross_Invoice_Amount,
    IIF([Gross Invoice Amount] % 10 > 0, 1, 0) AS NotZeroEnding_Gross_Invoice_Amount,

    [Gross Invoice Amount Local] AS Gross_Invoice_Amount_Local,
    IIF([Gross Invoice Amount Local] % 1 > 0, 1, 0) AS HasFrac_Gross_Invoice_Amount_Local,
    IIF([Gross Invoice Amount Local] % 10 > 0, 1, 0) AS NotZeroEnding_Gross_Invoice_Amount_Local,

    [Check Amount] AS Check_Amount,
    IIF([Check Amount] % 1 > 0, 1, 0) AS HasFrac_Check_Amount,
    IIF([Check Amount] % 10 > 0, 1, 0) AS NotZeroEnding_Check_Amount,

    [Absolute Amount] AS Absolute_Amount,
    IIF([Absolute Amount] % 1 > 0, 1, 0) AS HasFrac_Absolute_Amount,
    IIF([Absolute Amount] % 10 > 0, 1, 0) AS NotZeroEnding_Absolute_Amount,

    Max_Abs_Amount,
    Min_Abs_Amount,

-- Spreads
    MAX(try_cast([Document Number] AS BIGINT))
        OVER ( PARTITION BY base_table.Report_Group_Flag, base_table.ReportID) -
    MIN(try_cast([Document Number] AS BIGINT))
        OVER ( PARTITION BY base_table.Report_Group_Flag, base_table.ReportID) AS Doc_Num_Spread,
    MAX(try_cast([Payment Document Number] AS BIGINT))
        OVER ( PARTITION BY base_table.Report_Group_Flag, base_table.ReportID) -
    MIN(try_cast([Payment Document Number] AS BIGINT))
        OVER ( PARTITION BY base_table.Report_Group_Flag, base_table.ReportID) AS PaymentDoc_Num_Spread,
    MAX(try_cast([Voucher Number] AS BIGINT))
        OVER ( PARTITION BY base_table.Report_Group_Flag, base_table.ReportID) -
    MIN(try_cast([Voucher Number] AS BIGINT))
        OVER ( PARTITION BY base_table.Report_Group_Flag, base_table.ReportID) AS Voucher_Num_Spread,

-- Indicator Variables
    IIF([Void Indicator] = 'CLEARED', 1, 0) AS Void_Indicator__CLEARED,
    IIF([Void Indicator] = 'VOIDED', 1, 0) AS Void_Indicator__VOIDED,
    IIF([Void Indicator] = 'RECONCILED', 1, 0) AS Void_Indicator__RECONCILED,
    IIF([Void Indicator] = 'NEGOTIABLE', 1, 0) AS Void_Indicator__NEGOTIABLE,
    IIF([Void Indicator] = '0', 1, 0) AS Void_Indicator__0,
    IIF([Void Indicator] = 'NULL') AS Void_Indicator__NULL,
    IIF([Void Indicator] NOT IN ('CLEARED', 'VOIDED', 'RECONCILED', 'NEGOTIABLE', '0'), 1, 0) AS Void_Indicator__OTH,
    IIF(ReportGroup = '1', 1, 0) AS ReportGroup__1,
    IIF(ReportGroup = '2', 1, 0) AS ReportGroup__2,
    IIF(ReportGroup = '3', 1, 0) AS ReportGroup__3,
    IIF(ReportGroup = '4', 1, 0) AS ReportGroup__4,
    IIF(ReportGroup = '5', 1, 0) AS ReportGroup__5,
    IIF(ReportGroup = '6', 1, 0) AS ReportGroup__6,
    IIF(ReportGroup = '7', 1, 0) AS ReportGroup__7,
    IIF(ReportGroup = '8', 1, 0) AS ReportGroup__8,
    IIF(ReportGroup = '9', 1, 0) AS ReportGroup__9,
    IIF(ReportGroup = '10', 1, 0) AS ReportGroup__10,
    IIF(base_table.ReportID = '1a', 1, 0) AS ReportID__1A,
    IIF(base_table.ReportID = '1b', 1, 0) AS ReportID__1B,
    IIF(base_table.ReportID = '1c', 1, 0) AS ReportID__1C,
    IIF(base_table.ReportID = '1d', 1, 0) AS ReportID__1D,
    IIF(base_table.ReportID = '2a', 1, 0) AS ReportID__2A,
    IIF(base_table.ReportID = '2b', 1, 0) AS ReportID__2B,
    IIF(base_table.ReportID = '2c', 1, 0) AS ReportID__2C,
    IIF(base_table.ReportID = '2d', 1, 0) AS ReportID__2D,
    IIF(base_table.ReportID = '3a', 1, 0) AS ReportID__3A,
    IIF(base_table.ReportID = '3b', 1, 0) AS ReportID__3B,
    IIF(base_table.ReportID = '3c', 1, 0) AS ReportID__3C,
    IIF(base_table.ReportID = '3d', 1, 0) AS ReportID__3D,
    IIF(base_table.ReportID = '4a', 1, 0) AS ReportID__4A,
    IIF(base_table.ReportID = '4b', 1, 0) AS ReportID__4B,
    IIF(base_table.ReportID = '4c', 1, 0) AS ReportID__4C,
    IIF(base_table.ReportID = '4d', 1, 0) AS ReportID__4D,
    IIF(base_table.ReportID = '5a', 1, 0) AS ReportID__5A,
    IIF(base_table.ReportID = '5b', 1, 0) AS ReportID__5B,
    IIF(base_table.ReportID = '5c', 1, 0) AS ReportID__5C,
    IIF(base_table.ReportID = '5d', 1, 0) AS ReportID__5D,
    IIF(base_table.ReportID = '6a', 1, 0) AS ReportID__6A,
    IIF(base_table.ReportID = '6b', 1, 0) AS ReportID__6B,
    IIF(base_table.ReportID = '6c', 1, 0) AS ReportID__6C,
    IIF(base_table.ReportID = '6d', 1, 0) AS ReportID__6D,
    IIF(base_table.ReportID = '7a', 1, 0) AS ReportID__7A,
    IIF(base_table.ReportID = '7b', 1, 0) AS ReportID__7B,
    IIF(base_table.ReportID = '7c', 1, 0) AS ReportID__7C,
    IIF(base_table.ReportID = '7d', 1, 0) AS ReportID__7D,
    IIF(base_table.ReportID = '8a', 1, 0) AS ReportID__8A,
    IIF(base_table.ReportID = '8b', 1, 0) AS ReportID__8B,
    IIF(base_table.ReportID = '8c', 1, 0) AS ReportID__8C,
    IIF(base_table.ReportID = '8d', 1, 0) AS ReportID__8D,
    IIF(base_table.ReportID = '9a', 1, 0) AS ReportID__9A,
    IIF(base_table.ReportID = '9b', 1, 0) AS ReportID__9B,
    IIF(base_table.ReportID = '9c', 1, 0) AS ReportID__9C,
    IIF(base_table.ReportID = '9d', 1, 0) AS ReportID__9D,
    IIF(base_table.ReportID = '10a', 1, 0) AS ReportID__10A,
    IIF(base_table.ReportID = '10b', 1, 0) AS ReportID__10B,
    IIF(base_table.ReportID = '10c', 1, 0) AS ReportID__10C,
    IIF(base_table.ReportID = '10d', 1, 0) AS ReportID__10D,
    IIF([Document Type] = '0', 1, 0) AS Document_Type__0,
    IIF([Document Type] = '1', 1, 0) AS Document_Type__1,
    IIF([Document Type] = '2', 1, 0) AS Document_Type__2,
    IIF([Document Type] = 'APINV', 1, 0) AS Document_Type__APINV,
    IIF([Document Type] = 'DO', 1, 0) AS Document_Type__DO,
    IIF([Document Type] = 'KA', 1, 0) AS Document_Type__KA,
    IIF([Document Type] = 'KN', 1, 0) AS Document_Type__KN,
    IIF([Document Type] = 'KR', 1, 0) AS Document_Type__KR,
    IIF([Document Type] = 'OTH', 1, 0) AS Document_Type__OTH,
    IIF([Document Type] = 'RE', 1, 0) AS Document_Type__RE,
    IIF([Document Type] = 'RM', 1, 0) AS Document_Type__RM,
    IIF([Document Type] = 'RN', 1, 0) AS Document_Type__RN,
    IIF([Document Type] = 'XK', 1, 0) AS Document_Type__XK,
    IIF([Document Type] = 'YB', 1, 0) AS Document_Type__YB,
    IIF([Document Type] = 'YI', 1, 0) AS Document_Type__YI,
    IIF([Document Type] = 'YR', 1, 0) AS Document_Type__YR,
    IIF([Document Type] = 'YT', 1, 0) AS Document_Type__YT,
    IIF([Document Type] = 'Z1', 1, 0) AS Document_Type__Z1,
    IIF([Document Type] = 'ZF', 1, 0) AS Document_Type__ZF,
    IIF([Document Type] = 'ZG', 1, 0) AS Document_Type__ZG,
    IIF(AmountNet = 'N/A', 1, 0) AS AmountNet__NA,
    IIF(AmountNet = 'NET-ONE', 1, 0) AS AmountNet__NETONE,
    IIF(AmountNet = 'NET-ZERO', 1, 0) AS AmountNet__NETZERO

FROM {}.dbo.{} AS base_table
    LEFT OUTER JOIN (
    SELECT Report_Group_Flag,
    ReportID,
    MF_DocType,
    MF_DocType_Pct
    FROM (
    SELECT row_num,
    Report_Group_Flag,
    ReportID,
    MF_DocType,
    IIF(SUM (DocType_Count)
    OVER (PARTITION BY Report_Group_Flag, ReportID) > 0, (
    (DocType_Count * 1.0) /
    (SUM (DocType_Count)
    OVER (PARTITION BY Report_Group_Flag, ReportID) *
    1.0)
    ), NULL) AS MF_DocType_Pct
    FROM (SELECT Report_Group_Flag,
    ReportID,
     [Document Type] AS MF_DocType,
    COUNT (
     [Document Type]) AS DocType_Count,
    row_number()
    OVER (PARTITION BY Report_Group_Flag, ReportID ORDER BY COUNT (
     [Document Type]) DESC) AS row_num
    FROM {}.dbo.{}
    WHERE
     [Document Type] IS NOT NULL
    GROUP BY Report_Group_Flag, ReportID,
     [Document Type]
    ) t
    ) t
    WHERE row_num = 1
    ) AS doctypes
ON base_table.Report_Group_Flag = doctypes.Report_Group_Flag
    AND base_table.ReportID = doctypes.ReportID
