package com.iainschmitt.puro

import kotlinx.serialization.Serializable

@Serializable
data class FlagData(
    val id: Int,
    val name: String,
    val code: String,
    val code3: String,
    val numeric: String,
    val emoji: String,
    val unicode: String,
    val svg_url: String,
    val png_url: String,
    val cdn_urls: CdnUrls,
    val css_class: String,
    val colors: Colors,
    val aspect_ratio: String,
    val emoji_qualified: Boolean
)

@Serializable
data class CdnUrls(
    val flagcdn: CdnProvider,
    val flagpedia: CdnProvider,
    val countryflagsapi: CdnProvider
)

@Serializable
data class CdnProvider(
    val svg: String,
    val png: String,
    val png_small: String,
    val png_large: String
)

@Serializable
data class Colors(
    val primary: String,
    val secondary: String,
    val tertiary: String? = null,
    val quaternary: String? = null
)
