import json
import logging
from pathlib import Path
from typing import Dict, Any, List

import requests
import semanticscholar
import wget
from bs4 import BeautifulSoup

from papercast.types import PathLike, PDFFile
from papercast.production import Production
from papercast.base import BaseProcessor


class SemanticScholarProcessor(BaseProcessor):
    input_types = {"corpus_id": str}

    output_types = {
            "pdf": PDFFile,
            "title": str,
            "authors": List,
            "doi": str,
            "description": str,
        }

    def __init__(self, pdf_dir: PathLike, json_dir: PathLike, timeout: int = 10):
        self.pdf_dir = Path(pdf_dir)
        self.timeout = timeout

    def process(self, production: Production, **kwargs) -> Production:
        pdf_path, doc = self._download(production.corpus_id) # type: ignore

        setattr(production, "pdf", PDFFile(path=pdf_path))

        for k, v in doc.items():
            setattr(production, k, v)

        return production

    def _get_pdf_link_semantic_scholar(self, paper: dict):
        page = requests.get(paper["url"])
        soup = BeautifulSoup(page.content, "html.parser")
        paper_link = soup.find("a", {"data-selenium-selector": "paper-link"})
        if paper_link:
            paper_link = paper_link["href"] # type: ignore
            if "pdf" in paper_link:
                return paper_link
            else:
                logging.info(f"No pdf link found for paper")
                return None
        else:
            raise Exception("Could not get paper link")

    def _download(
        self,
        corpus_id: str,
    ):
        paper = semanticscholar.SemanticScholar(timeout=self.timeout).paper( # type: ignore
            f"CorpusID:{corpus_id}"
        )  # type: dict

        pdf_link = self._get_pdf_link_semantic_scholar(paper)

        if pdf_link is not None:
            pdf_path = str(self.pdf_dir / f"{corpus_id}.pdf")
            wget.download(pdf_link, pdf_path)
        else:
            raise Exception("Could not get pdf link")

        doc = {
            "outpath": pdf_path,
            "title": paper["title"],
            "corpus_id": corpus_id,
            "authors": [author["name"] for author in paper["authors"]],
            "doi": paper["doi"],
            "description": paper["abstract"].replace("\n", " "),
        }

        logging.info(f"Downloaded pdf to {pdf_path}")

        return pdf_path, doc
