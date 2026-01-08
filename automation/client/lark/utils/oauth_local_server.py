#coding:utf-8
"""Local OAuth Callback Server for Desktop/CLI Applications

This module provides a minimal local HTTP server to handle OAuth redirect callbacks.
It focuses solely on receiving the authorization code from the OAuth provider.

Usage:
    # Programmatic usage
    from automation.client.lark.utils.oauth_local_server import OAuthCallbackServer
    
    server = OAuthCallbackServer(port=8080)
    server.start()
    
    # ... open authorization URL in browser ...
    
    auth_code, state = server.wait_for_callback(timeout=120)
    server.stop()
    
    # Command-line usage
    python -m automation.client.lark.utils.oauth_local_server --port 8080
"""

import logging
import socket
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from typing import Optional, Tuple

logger = logging.getLogger("automation.lark.oauth_local_server")


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP request handler for OAuth callback"""
    
    # Class variables to store authorization result
    auth_code: Optional[str] = None
    auth_state: Optional[str] = None
    auth_error: Optional[str] = None
    
    def do_GET(self):
        """Handle GET request from OAuth callback"""
        query_components = parse_qs(urlparse(self.path).query)
        
        # Check for authorization code
        if 'code' in query_components:
            OAuthCallbackHandler.auth_code = query_components['code'][0]
            OAuthCallbackHandler.auth_state = query_components.get('state', [None])[0]
            
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            
            success_html = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <title>授权成功</title>
                <style>
                    body { font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f4f4f4; }
                    .container { background: white; padding: 40px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); max-width: 500px; margin: 0 auto; }
                    h1 { color: #00b96b; }
                    p { color: #666; line-height: 1.6; }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>✓ 授权成功！</h1>
                    <p>您已成功完成授权，现在可以关闭此窗口。</p>
                    <p>程序将自动继续执行...</p>
                </div>
            </body>
            </html>
            """
            self.wfile.write(success_html.encode('utf-8'))
            logger.info("Successfully received authorization code")
            
        elif 'error' in query_components:
            OAuthCallbackHandler.auth_error = query_components['error'][0]
            
            self.send_response(400)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            
            error_html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <title>授权失败</title>
                <style>
                    body {{ font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f4f4f4; }}
                    .container {{ background: white; padding: 40px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); max-width: 500px; margin: 0 auto; }}
                    h1 {{ color: #f5222d; }}
                    p {{ color: #666; line-height: 1.6; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>✗ 授权失败</h1>
                    <p>错误信息: {OAuthCallbackHandler.auth_error}</p>
                    <p>您可以关闭此窗口并重试。</p>
                </div>
            </body>
            </html>
            """
            self.wfile.write(error_html.encode('utf-8'))
            logger.error(f"Authorization failed with error: {OAuthCallbackHandler.auth_error}")
        else:
            self.send_response(400)
            self.end_headers()
    
    def log_message(self, format, *args):
        """Suppress default HTTP server logs"""
        pass


def start_local_server(port: int = 8080, timeout: int = 120) -> Tuple[Optional[str], Optional[str]]:
    """Start local HTTP server and wait for OAuth callback
    
    Args:
        port: Port number for local server (default: 8080)
        timeout: Timeout in seconds (default: 120)
        
    Returns:
        Tuple of (auth_code, state) or (None, None) if timeout
        
    Raises:
        Exception: If authorization fails with error
        TimeoutError: If no callback received within timeout
        OSError: If port is already in use
    """
    # Reset class variables
    OAuthCallbackHandler.auth_code = None
    OAuthCallbackHandler.auth_state = None
    OAuthCallbackHandler.auth_error = None
    
    try:
        server = HTTPServer(('localhost', port), OAuthCallbackHandler)
        # Allow socket reuse to avoid "Address already in use" errors
        server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except OSError as e:
        if e.errno == 48:  # Address already in use
            raise OSError(f"Port {port} is already in use. Please use a different port with --port option") from e
        raise
    
    # Start server in separate thread
    server_thread = threading.Thread(target=server.handle_request, daemon=True)
    server_thread.start()
    
    logger.info(f"Local OAuth server started on http://localhost:{port}")
    
    # Wait for callback with timeout
    server_thread.join(timeout=timeout)
    server.server_close()
    
    if OAuthCallbackHandler.auth_code:
        return OAuthCallbackHandler.auth_code, OAuthCallbackHandler.auth_state
    elif OAuthCallbackHandler.auth_error:
        raise Exception(f"OAuth authorization failed: {OAuthCallbackHandler.auth_error}")
    else:
        raise TimeoutError(f"Authorization timeout after {timeout} seconds")


class OAuthCallbackServer:
    """Reusable OAuth callback server with context manager support
    
    Example:
        >>> server = OAuthCallbackServer(port=8080)
        >>> server.start()
        >>> # Open authorization URL in browser
        >>> code, state = server.wait_for_callback(timeout=120)
        >>> server.stop()
        
        # Or use as context manager
        >>> with OAuthCallbackServer(port=8080) as server:
        ...     # Open authorization URL
        ...     code, state = server.wait_for_callback()
    """
    
    def __init__(self, port: int = 8080):
        """Initialize callback server
        
        Args:
            port: Port number for local server
        """
        self.port = port
        self.server: Optional[HTTPServer] = None
        self.server_thread: Optional[threading.Thread] = None
        self._started = False
    
    def start(self):
        """Start the HTTP server in background thread"""
        if self._started:
            logger.warning("Server already started")
            return
        
        # Reset handler state
        OAuthCallbackHandler.auth_code = None
        OAuthCallbackHandler.auth_state = None
        OAuthCallbackHandler.auth_error = None
        
        try:
            self.server = HTTPServer(('localhost', self.port), OAuthCallbackHandler)
            # Allow socket reuse
            self.server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except OSError as e:
            if e.errno == 48:  # Address already in use
                raise OSError(
                    f"Port {self.port} is already in use. "
                    f"Please try a different port or kill the process using: lsof -ti:{self.port} | xargs kill"
                ) from e
            raise
        
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self._started = True
        
        logger.info(f"OAuth callback server started on http://localhost:{self.port}")
    
    def wait_for_callback(self, timeout: int = 120) -> Tuple[Optional[str], Optional[str]]:
        """Wait for OAuth callback and return auth code
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            Tuple of (auth_code, state)
            
        Raises:
            Exception: If authorization fails
            TimeoutError: If timeout reached
        """
        if not self._started:
            raise RuntimeError("Server not started. Call start() first.")
        
        start_time = threading.Event()
        
        def check_callback():
            """Check if callback received"""
            while not start_time.wait(0.5):
                if OAuthCallbackHandler.auth_code or OAuthCallbackHandler.auth_error:
                    return
        
        check_thread = threading.Thread(target=check_callback, daemon=True)
        check_thread.start()
        check_thread.join(timeout=timeout)
        
        if OAuthCallbackHandler.auth_code:
            logger.info("Successfully received authorization code")
            return OAuthCallbackHandler.auth_code, OAuthCallbackHandler.auth_state
        elif OAuthCallbackHandler.auth_error:
            raise Exception(f"OAuth authorization failed: {OAuthCallbackHandler.auth_error}")
        else:
            raise TimeoutError(f"Authorization timeout after {timeout} seconds")
    
    def stop(self):
        """Stop the HTTP server"""
        if self.server and self._started:
            self.server.shutdown()
            self.server.server_close()
            logger.info("OAuth callback server stopped")
            self._started = False
    
    @property
    def redirect_uri(self) -> str:
        """Get the redirect URI for this server"""
        return f"http://localhost:{self.port}/callback"
    
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()
        return False




def main():
    """Test OAuth callback server - standalone mode
    
    Usage:
        python oauth_local_server.py --port 8080 --timeout 120
        
    Then open your authorization URL in browser manually.
    The server will display the received authorization code.
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='OAuth Callback Server - receives authorization code from redirect',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  # Start server on port 8080
  python oauth_local_server.py --port 8080
  
  # Then open authorization URL in browser with redirect_uri=http://localhost:8080/callback
  # Server will print the received authorization code
        """
    )
    
    parser.add_argument('--port', type=int, default=8080, 
                       help='Server port (default: 8080)')
    parser.add_argument('--timeout', type=int, default=300, 
                       help='Timeout in seconds (default: 300)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print(f"\n{'='*60}")
    print(f"OAuth Callback Server")
    print(f"{'='*60}")
    print(f"Server: http://localhost:{args.port}/callback")
    print(f"Timeout: {args.timeout} seconds")
    print(f"\nWaiting for authorization callback...")
    print(f"{'='*60}\n")
    
    try:
        code, state = start_local_server(port=args.port, timeout=args.timeout)
        
        print(f"\n{'='*60}")
        print(f"✓ Authorization Code Received")
        print(f"{'='*60}")
        print(f"Code: {code}")
        if state:
            print(f"State: {state}")
        print(f"{'='*60}\n")
        
        return 0
        
    except TimeoutError as e:
        print(f"\n✗ {e}\n")
        return 1
    except Exception as e:
        print(f"\n✗ Error: {e}\n")
        logger.exception("Callback server failed")
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())

