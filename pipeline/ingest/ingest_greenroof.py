"""
Combined greenroof ingest - calls both Empower and Kissel ingesters.
"""
from pipeline.ingest.ingest_greenroof_empower import ingest_empower_greenroof
from pipeline.ingest.ingest_greenroof_kissel import ingest_kissel_greenroof


def ingest_greenroof():
    '''Ingest all greenroof data from both sources.'''
    print('[ingest_greenroof] Starting Empower ingest...')
    ingest_empower_greenroof()
    
    print('[ingest_greenroof] Starting Kissel ingest...')
    ingest_kissel_greenroof()
    
    print('[ingest_greenroof] All greenroof data ingested.')
