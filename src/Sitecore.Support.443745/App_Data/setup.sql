-- ================================================
-- Template generated from Template Explorer using:
-- Create Procedure (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
-- the definition of the procedure.
-- ================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE UpsertDbProperty
	-- Add the parameters for the stored procedure here
	@key nvarchar(256),
	@value ntext
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    MERGE Properties AS target
    USING (SELECT @key, @value) AS source ([Key], [Val])
	on target.[Key]=source.[key]
	 WHEN MATCHED THEN 
        UPDATE SET value = source.[Val]
	WHEN NOT MATCHED THEN
    INSERT ([Key], Value)
		VALUES (source.[Key], source.[Val]);
END
GO

