try:
    from .app import main
except ImportError:
    # Fallback for environments that execute this file as a top-level script.
    from stock_screener.app import main


if __name__ == "__main__":
    main()
