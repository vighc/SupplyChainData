# Supply Chain Data Engineering Project

## Assumptions

#### Order Types:
- Bill To: Represents the customer who placed the order. (Code: 32604)
- Ship To: Represents the location where the shipment needs to be transferred.

#### Handling Orders:
- Create a new Ship To order to deliver the billing invoice to the customer's location. *(invoice_df | id=1 | outputData)*
- Keep all Ship To orders as they are. *(shipto_df | id=2-75 | outputData)*
- Keep the original Bill To transaction for maintaining clarity. *(billto_df | id=76 | outputData)*

#### Assuming all shipments belong to a single customer due to insufficient linkings.

#### Assuming that the zip in the format `xxxxx-yyyy` is ‘US’ and `xxxxx` format is ‘USA’ as a country.

## Logic

1. **Initialize Spark Session**: Start a Spark session for data processing.
2. **Read Excel File**: Use pandas to read the file and convert to Spark DataFrame.
3. **Replace NaN Values**: Handle missing data by replacing "NaN" with None.
4. **Create Customer DataFrame**: Filter Bill To orders and create a DataFrame with customer details.
5. **Create Invoice DataFrame**: Change Bill To to Ship To for new shipment records.
6. **Create Ship To DataFrame**: Ensure all Ship To orders are linked to the customer.
7. **Create Bill To DataFrame**: Set Code to None to avoid shipment and distinguish from original Bill To orders.
8. **Combine DataFrames**: Consolidate Invoice, Ship To, and Bill To DataFrames.
9. **Mapping to Standard Layout**: Add and rename columns for consistency and clarity.
10. **Convert to Pandas DataFrames**: Prepare for writing to Excel.
11. **Write to Excel**: Save the final data with different sheets for outputData and customer.

Above mentioned exact steps can be found in the code.

## Conclusion
This project aims to streamline the handling of `Bill To` and `Ship To` orders in supply chain data engineering. By following the logic outlined above, we ensure accurate and efficient data processing, leading to better management and clarity in order handling.

Feel free to contribute and improve the code. Happy coding!
