# CI6226_INFORMATION_RETRIEVAL_ANALYSIS_2021_SP
CI6226 INFORMATION RETRIEVAL &amp; ANALYSIS at NTU in 2021 SP.

## Indexing Component

The indexing component uses SPIMI algorithm to sort the documents, whose inputs are the path and block size for SPIMI and returns a single text file contains a sorted list of term-document pairs.

### Files Reading, Tokenisation and Compression

To reduce the time transferring, this component contains one class to read files, tokenise them and compression them in one class. And there is also another function to return the compressed tokens.

The compression methods including removing stop words, removing numbers, case folding, stemming and punctuation symbols which benefits from the Python lib NLTK.

### Sorting

The compressed tokens will be sent to the SPIMI sorting component and write the sorted term-document pairs inside the class.