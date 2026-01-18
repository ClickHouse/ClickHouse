SELECT idx, (price * 0.9) + 5 AS discounted_price_with_tax FROM products WHERE stock > (10 + 20);
