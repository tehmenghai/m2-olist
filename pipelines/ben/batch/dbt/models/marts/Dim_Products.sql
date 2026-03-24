SELECT
    p.product_id,
    p.product_category_name,
    CASE p.product_category_name
        WHEN 'cama_mesa_banho' THEN 'Bed Bath Table'
        WHEN 'beleza_saude' THEN 'Health Beauty'
        WHEN 'esporte_lazer' THEN 'Sports Leisure'
        WHEN 'moveis_decoracao' THEN 'Furniture Decor'
        WHEN 'informatica_acessorios' THEN 'Computers Accessories'
        WHEN 'utilidades_domesticas' THEN 'Housewares'
        WHEN 'relogios_presentes' THEN 'Watches Gifts'
        WHEN 'telefonia' THEN 'Telephony'
        WHEN 'ferramentas_jardim' THEN 'Garden Tools'
        WHEN 'automotivo' THEN 'Auto'
        ELSE COALESCE(p.product_category_name, 'Other')
    END                          AS product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    ROUND((p.product_length_cm * p.product_height_cm * p.product_width_cm) / 1000, 2) AS volume_litres
FROM {{ ref('stg_products') }} p
