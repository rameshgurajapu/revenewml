WITH invoice_level AS (SELECT [Vendor Number] AS Vendor_Number,

                              -- Invoice Number Variables
                              len([Invoice Number]) AS Vendor_Invoice_Number_Length,
                              try_cast([Invoice Number] AS BIGINT) AS Vendor_Invoice_Number_Numeric,
                              len(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 50)) /
                              (len([Invoice Number]) * 1.0) AS Vendor_Invoice_Number_AlphasPct,
                              1 - (len(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 50)) /
                                   (len([Invoice Number]) * 1.0)) AS Vendor_Invoice_Number_NumsPct,
                              IIF(len(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 50)) !=
                                  len(try_cast(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]),
                                                         50) AS BIGINT)),
                                  len(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]), 50)) -
                                  len(try_cast(substring([Invoice Number], patindex('%[^a-zA-Z]%', [Invoice Number]),
                                                         50) AS BIGINT)),
                                  NULL) AS Vendor_Invoice_Number_LeadingZeroes,

                              -- Daily Invoice Count
                              COUNT([Invoice Date]) OVER ( PARTITION BY [Vendor Number]) AS Vendor_Daily_Invoice_Count,

                              -- Date Variables
                              datediff(D, [Invoice Date], [Check Date]) AS Vendor_Days_Invoice_to_Check_Date,
                              datediff(D, [Invoice Date], [Clearing Date]) AS Vendor_Days_Invoice_to_Clearing_Date,
                              datediff(D, [Invoice Date], [Posting Date]) AS Vendor_Days_Invoice_to_Posting_Date,
                              datediff(D, [Invoice Date], [Void Date]) AS Vendor_Days_Invoice_to_Void_Date,

                              -- Amount Variables
                              [Gross Invoice Amount] AS Vendor_Gross_Invoice_Amount,
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%1%', 1, 0) +
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%2%', 1, 0) +
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%3%', 1, 0) +
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%4%', 1, 0) +
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%5%', 1, 0) +
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%6%', 1, 0) +
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%7%', 1, 0) +
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%8%', 1, 0) +
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%9%', 1, 0) +
                              IIF(try_cast([Gross Invoice Amount] AS VARCHAR) LIKE '%0%', 1, 0)
                                  AS Vendor_UniqueDigits_Gross_Invoice_Amount,

                              IIF(try_cast([Gross Invoice Amount] as NUMERIC) % 1 > 0, 1, 0) AS Vendor_HasFrac_Gross_Invoice_Amount,
                              IIF(try_cast([Gross Invoice Amount] as NUMERIC) % 10 > 0, 1, 0) AS Vendor_NotZeroEnding_Gross_Invoice_Amount,

                              [Gross Invoice Amount Local] AS Vendor_Gross_Invoice_Amount_Local,
                              IIF(try_cast([Gross Invoice Amount Local] as NUMERIC) % 1 > 0, 1, 0) AS Vendor_HasFrac_Gross_Invoice_Amount_Local,
                              IIF(try_cast([Gross Invoice Amount Local] as NUMERIC) % 10 > 0, 1, 0) AS Vendor_NotZeroEnding_Gross_Invoice_Amount_Local,

                              [Check Amount] AS Vendor_Check_Amount,
                              IIF(try_cast([Check Amount] as NUMERIC) % 1 > 0, 1, 0) AS Vendor_HasFrac_Check_Amount,
                              IIF(try_cast([Check Amount] as NUMERIC) % 10 > 0, 1, 0) AS Vendor_NotZeroEnding_Check_Amount,

                              [Absolute Amount] AS Vendor_Absolute_Amount,
                              IIF(try_cast([Absolute Amount] as NUMERIC) % 1 > 0, 1, 0) AS Vendor_HasFrac_Absolute_Amount,
                              IIF(try_cast([Absolute Amount] as NUMERIC) % 10 > 0, 1, 0) AS Vendor_NotZeroEnding_Absolute_Amount,

                              -- Spreads
                              MAX(try_cast([Document Number] AS BIGINT)) OVER ( PARTITION BY [Vendor Number]) -
                              MIN(try_cast([Document Number] AS BIGINT))
                                  OVER ( PARTITION BY [Vendor Number]) AS Vendor_Doc_Num_Spread,
                              MAX(try_cast([Payment Document Number] AS BIGINT)) OVER ( PARTITION BY [Vendor Number]) -
                              MIN(try_cast([Payment Document Number] AS BIGINT))
                                  OVER ( PARTITION BY [Vendor Number]) AS Vendor_PaymentDoc_Num_Spread,
                              MAX(try_cast([Voucher Number] AS BIGINT)) OVER ( PARTITION BY [Vendor Number]) -
                              MIN(try_cast([Voucher Number] AS BIGINT))
                                  OVER ( PARTITION BY [Vendor Number]) AS Vendor_Voucher_Num_Spread

                      FROM {}.dbo.invoice AS base_table
--                        FROM Revenew.dbo.invoice AS base_table
                                LEFT OUTER JOIN (
                           SELECT Vendor_Number,
                                  MF_DocType,
                                  MF_DocType_Pct AS Vendor_MF_DocType_Pct
                           FROM (SELECT row_num,
                                        Vendor_Number,
                                        MF_DocType,
                                        (1.0 * DocType_Count) /
                                        SUM(DocType_Count) OVER (PARTITION BY Vendor_Number) AS MF_DocType_Pct
                                 FROM (SELECT row_number()
                                                      OVER (PARTITION BY [Vendor Number] ORDER BY COUNT([Document Type]) DESC) AS row_num,
                                              [Vendor Number] AS Vendor_Number,
                                              [Document Type] AS MF_DocType,
                                              COUNT([Document Type]) AS DocType_Count

                                      FROM {}.dbo.invoice as base_table
--                                        FROM Revenew.dbo.invoice as base_table
                                       WHERE [Document Type] IS NOT NULL
                                       GROUP BY [Vendor Number], [Document Type]
                                      ) t
                                ) t
                           WHERE row_num = 1
                       ) AS doctypes ON base_table.[Vendor Number] = doctypes.Vendor_Number)
SELECT * 
FROM invoice_level


