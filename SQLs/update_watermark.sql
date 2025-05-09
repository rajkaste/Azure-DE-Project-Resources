CREATE PROCEDURE UpdateWatermarkTable
    @lastload VARCHAR(200)
AS
BEGIN
    -- Start the transaction
    BEGIN TRANSACTION;

    -- Update the incremental column in the watermark table
        UPDATE water_table
        SET last_load = @lastload

    COMMIT TRANSACTION;

END;