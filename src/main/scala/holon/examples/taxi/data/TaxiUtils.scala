package holon.examples.taxi

object TaxiUtils {

    // Grid parameters for Query 2
    private val ORIGIN_LAT = 41.474937
    private val ORIGIN_LON = -74.913585

    // Coordinate conversion factors (from the document)
    // For 500m: south = 0.004491556 degrees, east = 0.005986 degrees
    private val DEGREES_PER_250M_SOUTH = 0.004491556 / 2.0 // 0.002245778
    private val DEGREES_PER_250M_EAST = 0.005986 / 2.0 // 0.002993

    // Grid bounds for Query 2 (600x600 cells)
    private val MAX_CELLS = 600

    /**
     * Converts latitude and longitude to cell ID for Query 2 (250m x 250m cells)
     *
     * @param lat Latitude coordinate (e.g., 40.725124)
     * @param lon Longitude coordinate (e.g., -73.99221)
     * @return Option[String] - Cell ID in format "X.Y" or None if outside grid bounds
     */
    def getCellIdQ2(lat: Double, lon: Double): Option[String] = {
        // Calculate the difference from origin
        val latDiff = ORIGIN_LAT - lat // Positive when moving south
        val lonDiff = lon - ORIGIN_LON // Positive when moving east

        // Convert coordinate differences to cell indices
        val southCells = math.round(latDiff / DEGREES_PER_250M_SOUTH).toInt
        val eastCells = math.round(lonDiff / DEGREES_PER_250M_EAST).toInt

        // Calculate cell coordinates (1-based indexing)
        val cellX = eastCells + 1
        val cellY = southCells + 1

        // Check if coordinates are within valid grid bounds
        if (cellX >= 1 && cellX <= MAX_CELLS && cellY >= 1 && cellY <= MAX_CELLS) {
            Some(s"$cellX.$cellY")
        } else {
            None // Outside grid bounds - treated as outlier
        }
    }

    // Example usage and testing
    def main(args: Array[String]): Unit = {
        // Test with the provided example coordinates
        val testLat = 40.725124
        val testLon = -73.99221

        println(s"Testing coordinates: ($testLat, $testLon)")

        val cellId = getCellIdQ2(testLat, testLon)
        println(s"Cell ID: ${cellId.getOrElse("OUTLIER")}")

        // Test with origin coordinates
        println(s"\nTesting origin coordinates: ($ORIGIN_LAT, $ORIGIN_LON)")
        val originCellId = getCellIdQ2(ORIGIN_LAT, ORIGIN_LON)
        println(s"Origin Cell ID: ${originCellId.getOrElse("OUTLIER")}")
    }

}
