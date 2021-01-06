CREATE   FUNCTION dbo.PrettyPrintJson(@Json NVARCHAR(Max), @NestingSpaces int = 4)
RETURNS NVARCHAR(Max)
WITH SCHEMABINDING
AS
BEGIN
  DECLARE @result NVARCHAR(MAX);

  WITH CTE_NumArray
  AS (SELECT v
      FROM (VALUES (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1),
                   (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1),
                   (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1),
                   (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1)
           )AS n(v)
     )
  ,CTE_Nums
  AS (SELECT Row_Number() OVER (ORDER BY (SELECT NULL)) AS Num
      FROM CTE_NumArray AS o1
      CROSS JOIN CTE_NumArray AS o2
      CROSS JOIN CTE_NumArray AS o3
      CROSS JOIN CTE_NumArray AS o4
     )
  ,CTE_Letters
  AS (SELECT n.Num
            ,Substring(@json, n.Num, 1) AS Letter
            ,CASE WHEN Substring(@json, n.Num -1, 2)IN ('\b', '\f', '\n', '\r', '\t', '\"', '\\')
                    THEN 1
                  ELSE 0
               END AS IsEscaped
      FROM CTE_Nums AS n
      WHERE n.num <= Len(@json)
     )
  
  ,CTE_QuotedValues
  AS (SELECT l.Num
            ,l.Letter
            -- We count the unescaped quotes, and it it is even we add a space after the colon. If it is odd, we don't want to change a string value!
            ,Sum(CASE WHEN l.Letter = '"' AND l.IsEscaped = 0 THEN 1 ELSE 0 END) OVER (ORDER BY l.Num RANGE UNBOUNDED PRECEDING) % 2 AS IsCharInQuotedValue
      FROM CTE_Letters AS l
     )
  ,CTE_BracketCount
  AS (SELECT qv.Num
            ,qv.Letter
            ,qv.IsCharInQuotedValue
            ,Sum(CASE WHEN qv.Letter IN ('[', '{') AND qv.IsCharInQuotedValue = 0 THEN 1 ELSE 0 END) OVER (ORDER BY qv.Num RANGE UNBOUNDED PRECEDING) AS OpenedBrackets
            ,Sum(CASE WHEN qv.Letter IN (']', '}') AND qv.IsCharInQuotedValue = 0 THEN 1 ELSE 0 END) OVER (ORDER BY qv.Num RANGE UNBOUNDED PRECEDING) AS ClosedBrackets
      FROM CTE_QuotedValues AS qv
     )
  ,CTE_FinalLetter
  AS (SELECT bc.Num
            ,CASE WHEN bc.Letter IN (',', '[', '{') AND bc.IsCharInQuotedValue = 0
                    THEN bc.Letter + Char(10) + Replicate(' ', (bc.OpenedBrackets - bc.ClosedBrackets) * @NestingSpaces)
                  WHEN bc.Letter IN (']', '}') AND bc.IsCharInQuotedValue = 0
                    THEN Char(10) + Replicate(' ', (bc.OpenedBrackets - bc.ClosedBrackets) * @NestingSpaces) + bc.Letter
                  WHEN bc.Letter = ':' AND bc.IsCharInQuotedValue = 0
                    THEN bc.Letter + ' '
                  ELSE bc.Letter 
               END AS FinalLetter
      FROM CTE_BracketCount AS bc
     )
  SELECT @result = String_Agg(fl.FinalLetter, '') WITHIN GROUP (ORDER BY fl.Num)
  FROM CTE_FinalLetter AS fl;

  RETURN @result;
END