-- ============================================================================
-- Sample data for the Retail DW star schema
-- Covers January–February 2014 across 8 stores, 12 products, 8 customers,
-- 6 promotions, and ~55 sales transactions.
-- The first 4 rows of fact_sales match the DDIA book example exactly.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- dim_store
-- ----------------------------------------------------------------------------
INSERT INTO retail_dw.dim_store VALUES
(1, 'WA', 'Seattle'),
(2, 'CA', 'San Francisco'),
(3, 'CA', 'Palo Alto'),
(4, 'WA', 'Tacoma'),
(5, 'NY', 'New York'),
(6, 'TX', 'Austin'),
(7, 'CA', 'Los Angeles'),
(8, 'WA', 'Bellevue');

-- ----------------------------------------------------------------------------
-- dim_product
-- ----------------------------------------------------------------------------
INSERT INTO retail_dw.dim_product VALUES
(30, 'OK4012', 'Bananas',           'Freshmax',  'Fresh fruit'),
(31, 'KA9511', 'Fish food',         'Aquatech',  'Pet supplies'),
(32, 'AB1234', 'Croissant',         'Dealicious', 'Bakery'),
(33, 'CD5678', 'Salmon fillet',     'Freshmax',  'Fresh fish'),
(34, 'EF9012', 'Cat food',          'Aquatech',  'Pet supplies'),
(35, 'GH3456', 'Sourdough bread',   'Dealicious', 'Bakery'),
(36, 'IJ7890', 'Apples',            'Freshmax',  'Fresh fruit'),
(37, 'KL2345', 'Dog treats',        'Aquatech',  'Pet supplies'),
(69, 'MN6789', 'Tropical fish',     'Aquatech',  'Pet supplies'),
(70, 'OP0123', 'Mangoes',           'Freshmax',  'Fresh fruit'),
(74, 'QR4567', 'Baguette',          'Dealicious', 'Bakery'),
(75, 'ST8901', 'Blueberry muffin',  'Dealicious', 'Bakery');

-- ----------------------------------------------------------------------------
-- dim_customer
-- ----------------------------------------------------------------------------
INSERT INTO retail_dw.dim_customer VALUES
(190, 'Alice',   '1979-03-29'),
(191, 'Bob',     '1961-09-02'),
(192, 'Cecil',   '1991-12-13'),
(193, 'Diana',   '1985-07-15'),
(194, 'Edward',  '1972-11-30'),
(195, 'Fiona',   '1995-04-22'),
(235, 'George',  '1988-06-10'),
(236, 'Hannah',  '2001-01-15');

-- ----------------------------------------------------------------------------
-- dim_promotion
-- ----------------------------------------------------------------------------
INSERT INTO retail_dw.dim_promotion VALUES
(18, 'New Year sale',        'Poster',       NULL),
(19, 'Aquarium deal',        'Direct mail',  'Leaflet'),
(20, 'Coffee & cake bundle', 'In-store sign', NULL),
(21, 'Fresh fruit week',     'TV',           'Digital coupon'),
(22, 'Pet care month',       'Radio',        'Paper coupon'),
(23, 'Weekend bake-off',     'In-store sign', 'Leaflet');

-- ----------------------------------------------------------------------------
-- dim_date  (January + early February 2014)
-- Jan 1  = Wednesday, New Year's Day (holiday)
-- Jan 20 = Monday, Martin Luther King Jr. Day (holiday)
-- ----------------------------------------------------------------------------
INSERT INTO retail_dw.dim_date VALUES
(140101, 2014, 'jan',  1, 'wed', 1),
(140102, 2014, 'jan',  2, 'thu', 0),
(140103, 2014, 'jan',  3, 'fri', 0),
(140104, 2014, 'jan',  4, 'sat', 0),
(140105, 2014, 'jan',  5, 'sun', 0),
(140106, 2014, 'jan',  6, 'mon', 0),
(140107, 2014, 'jan',  7, 'tue', 0),
(140108, 2014, 'jan',  8, 'wed', 0),
(140109, 2014, 'jan',  9, 'thu', 0),
(140110, 2014, 'jan', 10, 'fri', 0),
(140111, 2014, 'jan', 11, 'sat', 0),
(140112, 2014, 'jan', 12, 'sun', 0),
(140113, 2014, 'jan', 13, 'mon', 0),
(140114, 2014, 'jan', 14, 'tue', 0),
(140115, 2014, 'jan', 15, 'wed', 0),
(140116, 2014, 'jan', 16, 'thu', 0),
(140117, 2014, 'jan', 17, 'fri', 0),
(140118, 2014, 'jan', 18, 'sat', 0),
(140119, 2014, 'jan', 19, 'sun', 0),
(140120, 2014, 'jan', 20, 'mon', 1),
(140121, 2014, 'jan', 21, 'tue', 0),
(140122, 2014, 'jan', 22, 'wed', 0),
(140123, 2014, 'jan', 23, 'thu', 0),
(140124, 2014, 'jan', 24, 'fri', 0),
(140125, 2014, 'jan', 25, 'sat', 0),
(140126, 2014, 'jan', 26, 'sun', 0),
(140127, 2014, 'jan', 27, 'mon', 0),
(140128, 2014, 'jan', 28, 'tue', 0),
(140129, 2014, 'jan', 29, 'wed', 0),
(140130, 2014, 'jan', 30, 'thu', 0),
(140131, 2014, 'jan', 31, 'fri', 0),
(140201, 2014, 'feb',  1, 'sat', 0),
(140202, 2014, 'feb',  2, 'sun', 0),
(140203, 2014, 'feb',  3, 'mon', 0),
(140204, 2014, 'feb',  4, 'tue', 0),
(140205, 2014, 'feb',  5, 'wed', 0);

-- ----------------------------------------------------------------------------
-- fact_sales
-- Columns: date_key, product_sk, store_sk, promotion_sk, customer_sk,
--          quantity, net_price, discount_price
--
-- Rows 1–4 match the DDIA book example (page 93 / figure 3-11).
-- ----------------------------------------------------------------------------
INSERT INTO retail_dw.fact_sales VALUES
-- === Jan 1: New Year's Day (holiday) — New Year sale promotion active ===
(140101, 30, 1, 18, 190,   3, 0.59, 0.45),   -- Bananas, Seattle, NY sale, Alice
(140101, 32, 1, 18, NULL,  2, 2.99, 2.49),   -- Croissant, Seattle, NY sale, anon
(140101, 35, 2, 18, 192,   1, 3.49, 2.79),   -- Sourdough, SF, NY sale, Cecil
(140101, 69, 5, 18, NULL,  1, 19.99, 15.99), -- Tropical fish, NY, NY sale, anon
(140101, 36, 7, 21, 193,   5, 0.89, 0.69),   -- Apples, LA, fruit week, Diana
(140101, 74, 3, 20, NULL,  2, 4.49, 3.99),   -- Baguette, Palo Alto, cake bundle, anon

-- === Jan 2: Regular Thursday — four rows from DDIA book + extras ===
(140102, 31,  3, NULL, NULL,  1,  2.49,  2.49),  -- Fish food, Palo Alto, no promo, anon   [DDIA row 1]
(140102, 69,  5,   19, NULL,  3, 14.99,  9.99),  -- Tropical fish, NY, Aquarium deal, anon [DDIA row 2]
(140102, 74,  3,   23,  191,  1,  4.49,  3.89),  -- Baguette, Palo Alto, bake-off, Bob     [DDIA row 3]
(140102, 33,  8, NULL,  235,  4,  0.99,  0.99),  -- Salmon, Bellevue, no promo, George     [DDIA row 4]
(140102, 30,  4,   21,  194,  6,  0.59,  0.45),  -- Bananas, Tacoma, fruit week, Edward
(140102, 34,  6,   22, NULL,  2,  4.99,  4.49),  -- Cat food, Austin, pet care, anon
(140102, 75,  1,   20,  190,  3,  1.99,  1.79),  -- Muffin, Seattle, cake bundle, Alice
(140102, 36,  2,   21, NULL,  4,  0.89,  0.69),  -- Apples, SF, fruit week, anon

-- === Jan 3: Regular Friday ===
(140103, 32,  1, NULL,  191,  2,  2.99,  2.99),  -- Croissant, Seattle, no promo, Bob
(140103, 69,  5,   19,  192,  1, 14.99,  9.99),  -- Tropical fish, NY, Aquarium deal, Cecil
(140103, 33,  2, NULL, NULL,  2,  8.99,  8.99),  -- Salmon, SF, no promo, anon
(140103, 37,  8,   22,  235,  1,  3.49,  2.99),  -- Dog treats, Bellevue, pet care, George
(140103, 70,  7,   21, NULL,  3,  1.29,  0.99),  -- Mangoes, LA, fruit week, anon
(140103, 74,  4,   23,  193,  1,  4.49,  3.89),  -- Baguette, Tacoma, bake-off, Diana

-- === Jan 4: Saturday ===
(140104, 30,  1, NULL, NULL,  5,  0.59,  0.59),  -- Bananas, Seattle, no promo, anon
(140104, 31,  3,   19, NULL,  2,  2.49,  1.99),  -- Fish food, Palo Alto, Aquarium deal, anon
(140104, 35,  6, NULL,  194,  1,  3.49,  3.49),  -- Sourdough, Austin, no promo, Edward
(140104, 69,  5,   19,  191,  2, 14.99,  9.99),  -- Tropical fish, NY, Aquarium deal, Bob
(140104, 75,  2,   20,  195,  4,  1.99,  1.79),  -- Muffin, SF, cake bundle, Fiona
(140104, 36,  7, NULL, NULL,  3,  0.89,  0.89),  -- Apples, LA, no promo, anon

-- === Jan 5: Sunday ===
(140105, 33,  2, NULL,  190,  1,  8.99,  8.99),  -- Salmon, SF, no promo, Alice
(140105, 74,  1,   23, NULL,  2,  4.49,  3.89),  -- Baguette, Seattle, bake-off, anon
(140105, 34,  8,   22,  236,  1,  4.99,  4.49),  -- Cat food, Bellevue, pet care, Hannah
(140105, 30,  4, NULL, NULL,  8,  0.59,  0.59),  -- Bananas, Tacoma, no promo, anon

-- === Jan 6: Monday ===
(140106, 32,  3, NULL,  192,  1,  2.99,  2.99),  -- Croissant, Palo Alto, no promo, Cecil
(140106, 37,  6,   22, NULL,  3,  3.49,  2.99),  -- Dog treats, Austin, pet care, anon
(140106, 70,  7, NULL,  193,  2,  1.29,  1.29),  -- Mangoes, LA, no promo, Diana
(140106, 75,  5, NULL, NULL,  2,  1.99,  1.99),  -- Muffin, NY, no promo, anon
(140106, 33,  1, NULL,  194,  1,  8.99,  8.99),  -- Salmon, Seattle, no promo, Edward

-- === Jan 7: Tuesday ===
(140107, 31,  8,   19, NULL,  4,  2.49,  1.99),  -- Fish food, Bellevue, Aquarium deal, anon
(140107, 36,  2, NULL,  195,  5,  0.89,  0.89),  -- Apples, SF, no promo, Fiona
(140107, 74,  4, NULL, NULL,  1,  4.49,  4.49),  -- Baguette, Tacoma, no promo, anon
(140107, 34,  3,   22,  191,  2,  4.99,  4.49),  -- Cat food, Palo Alto, pet care, Bob

-- === Jan 20: MLK Day (holiday) ===
(140120, 30,  1, NULL, NULL, 10,  0.59,  0.49),  -- Bananas, Seattle, no promo, anon
(140120, 35,  2, NULL,  190,  2,  3.49,  3.49),  -- Sourdough, SF, no promo, Alice
(140120, 69,  5,   19, NULL,  2, 14.99,  9.99),  -- Tropical fish, NY, Aquarium deal, anon
(140120, 33,  7, NULL,  192,  1,  8.99,  8.99),  -- Salmon, LA, no promo, Cecil

-- === Feb 1: Saturday ===
(140201, 75,  1, NULL,  193,  3,  1.99,  1.99),  -- Muffin, Seattle, no promo, Diana
(140201, 30,  4, NULL, NULL,  6,  0.59,  0.59),  -- Bananas, Tacoma, no promo, anon
(140201, 37,  8, NULL,  235,  2,  3.49,  3.49),  -- Dog treats, Bellevue, no promo, George
(140201, 69,  6,   19, NULL,  1, 14.99, 11.99),  -- Tropical fish, Austin, Aquarium deal, anon

-- === Feb 3: Monday ===
(140203, 32,  2, NULL,  194,  2,  2.99,  2.99),  -- Croissant, SF, no promo, Edward
(140203, 74,  3,   23, NULL,  1,  4.49,  3.89),  -- Baguette, Palo Alto, bake-off, anon
(140203, 36,  7, NULL,  195,  4,  0.89,  0.89);  -- Apples, LA, no promo, Fiona
