WITH counts AS (SELECT Report_Group_Flag,
                       ReportGroup,
                       ReportID,
                       count(DISTINCT [Company Code]) AS Company_Code_Count,
                       count(DISTINCT [Invoice Number]) AS Invoice_Number_Count,
                       count(DISTINCT [Vendor Number]) AS Vendor_Number_Count,
                       count(DISTINCT [Vendor Name]) AS Vendor_Name_Count,
                       count(DISTINCT Whole_Dollar) AS Whole_Dollar_Count,
                       count(DISTINCT [Check Number]) AS Check_Number_Count,
                       count(DISTINCT [Payment Document Number]) AS Payment_Document_Number_Count,
                       count(DISTINCT [Gross Invoice Amount]) AS Gross_Invoice_Amount_Count,
                       count(DISTINCT [User Name]) AS User_Name_Count
               FROM {}.dbo.[Duplicate Reports]
--                 FROM Revenew.dbo.[Duplicate Reports]
                GROUP BY Report_Group_Flag, ReportGroup, ReportID),
     totals AS (SELECT Report_Group_Flag,
                       ReportGroup,
                       COUNT(*) AS Reports
               FROM {}.dbo.[Duplicate Reports]
--                 FROM Revenew.dbo.[Duplicate Reports]
                GROUP BY Report_Group_Flag, ReportGroup)
SELECT counts.Report_Group_Flag,
       counts.ReportGroup,
       counts.ReportID,
       Reports,
       Company_Code_Count / (1.0 * Reports) AS Prop_Company_Code_Count,
       Invoice_Number_Count / (1.0 * Reports) AS Prop_Invoice_Number_Count,
       Vendor_Number_Count / (1.0 * Reports) AS Prop_Vendor_Number_Count,
       Vendor_Name_Count / (1.0 * Reports) AS Prop_Vendor_Name_Count,
       Whole_Dollar_Count / (1.0 * Reports) AS Prop_Whole_Dollar_Count,
       Check_Number_Count / (1.0 * Reports) AS Prop_Check_Number_Count,
       Payment_Document_Number_Count / (1.0 * Reports) AS Prop_Payment_Document_Number_Count,
       Gross_Invoice_Amount_Count / (1.0 * Reports) AS Prop_Gross_Invoice_Amount_Count,
       User_Name_Count / (1.0 * Reports) AS Prop_User_Name_Count
FROM counts,
     totals
WHERE counts.Report_Group_Flag = totals.Report_Group_Flag
  AND counts.ReportGroup = totals.ReportGroup
;