# products_with_eq_deletes

This dataset can be used to test Iceberg scan with equality deletes.  It contains a partitioned dataset with 1000 rows, of which 550 have been deleted via equality and position deletes.

## Schema

| Name          | Type     | Comments                |
|---------------|----------|-------------------------|
| product_id    | int      | Unique key [ 0 .. 999 ] |
| name          | varchar  |                         |
| category      | varchar  | Partition column        |
| color         | varchar  |                         |
| created_date  | date     |                         |
| weight        | double   |                         |
| quantity      | int      |                         |

## Creation Steps

| Step                                                                                                                                 | Inserted Rows | Deleted Rows | Total rows |
|--------------------------------------------------------------------------------------------------------------------------------------|--------------:|-------------:|-----------:|
| Insert 200 rows with category 'widget', product_ids from [ 0 .. 199 ].                                                               |           200 |            0 |        200 |
| Delete product_ids [ 0 .. 29 ] via equality delete on product_id.                                                                    |             0 |           30 |        170 |
| Insert 200 rows with category 'gizmo', product_ids from [ 200 .. 399 ].                                                              |           200 |            0 |        370 |
| Delete all products with color 'green' via equality delete on color.                                                                 |             0 |           37 |        333 |
| Insert 600 rows, 200 each in categories 'widget', 'gizmo', 'gadget' with product_ids [ 400 .. 999 ]                                  |           600 |            0 |        933 |
| Delete product_ids [ 100 .. 199 ], [ 300 .. 399 ], [ 500 .. 599 ], [ 700 .. 799 ], [ 900 .. 999 ] via equality delete on product_id. |             0 |          480 |        453 |
| Delete product_ids [ 50 .. 52 ] via positional delete.                                                                               |             0 |            3 |        450 |
| *Total*                                                                                                                              |          1000 |          550 |        450 |
