from pydantic import BaseModel, Field


class AlsoBuyQueries(BaseModel):
    queries: list[str] = Field(
        ...,
        min_length=1,
        max_length=3,
        description="Concise search queries for complementary products.",
    )
    reasoninig: str = Field(
        ...,
        description="A very brief explanation of why these queries were suggested.",
    )


class Product(BaseModel):
    category_lvl_1: str = Field(..., description="Top-level product category.")
    category_lvl_2: str | None = Field(
        None, description="Second-level product category."
    )
    category_lvl_3: str | None = Field(
        None, description="Third-level product category."
    )
    category_lvl_4: str | None = Field(
        None, description="Fourth-level product category."
    )
    brand: str = Field(..., description="Brand of the product.")
    title: str = Field(..., description="Title of the product.")
    description: str = Field(..., description="Detailed product description.")
