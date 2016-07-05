#include "http.h"
#include <time.h>


  http_headers::http_headers ()
  {
  }

std::string http_headers::get_response(HandleKits &kits)
{
  std::stringstream ssOut;
  if(url == "/favicon.ico")
  {
     int nSize = 0;
     unsigned char* data = get_icon(&nSize);

     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "content-type: image/vnd.microsoft.icon" << std::endl;
     ssOut << "content-length: " << nSize << std::endl;
     ssOut << std::endl;

     ssOut.write((char*)data, nSize);
  }
  else if(url == "/")
  {
     std::string sHTML = "<html><body><h1>Hello World</h1><p>This is a test web server in c++</p></body></html>";
     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "content-type: text/html" << std::endl;
     ssOut << "content-length: " << sHTML.length() << std::endl;
     ssOut << std::endl;
     ssOut << sHTML;
  }
  else if(url == "/startkits")
  {
     std::string sHTML = "<html><body><h1>Starting Kits</h1></body></html>";
     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "content-type: text/html" << std::endl;
     ssOut << "content-length: " << sHTML.length() << std::endl;
     ssOut << std::endl;
     ssOut << sHTML;
     kits.runKits();
  }
  else if(url == "/getstats")
  {
     string kitsStats = kits.getStats();
     std::string sHTML = "<html><body><h1>Stats</h1></body></html>";
     ssOut << "HTTP/1.1 200 OK" << std::endl;
     ssOut << "content-type: text/html" << std::endl;
     ssOut << "content-length: " << kitsStats.length() << std::endl;
     ssOut << std::endl;
     ssOut <<   kitsStats;
  }
  else
  {
     std::string sHTML = "<html><body><h1>404 Not Found</h1><p>There's nothing here.</p></body></html>";
     ssOut << "HTTP/1.1 404 Not Found" << std::endl;
     ssOut << "content-type: text/html" << std::endl;
     ssOut << "content-length: " << sHTML.length() << std::endl;
     ssOut << std::endl;
     ssOut << sHTML;
  }
  return ssOut.str();
};


unsigned char* http_headers::get_icon(int* pOut)
{
      unsigned char icon_data[] = {
            //reserved
            0x00,0x00,
            //icon type (1 = icon)
            0x01,0x00,
            //number of
            //size of colour palette
            0x00,
            //reserved
            0x00,
            //colour planes (1)
            0x01,0x00,
            //bits per pixel (32)
            0x20,0x00,

            //size of data in bytes
            0x28,0x04,0x00,0x00,

            //offset of bitmap data
            0x16,0x00,0x00,0x00,

            //BEGIN BITMAPINFOHEADER
            //bcsize
            0x28,0x00,0x00,0x00, //biSize
            0x10,0x00,0x00,0x00, //biWidth
            0x20,0x00,0x00,0x00, //biHeight (with both AND and XOR mask? wtf?)

            0x01,0x00, //biPlanes
            0x20,0x00, //biBitCount (32)

            0x00,0x00,0x00,0x00, //biCompression
            0x00,0x00,0x00,0x00, //biSizeImage
            0x00,0x00,0x00,0x00, //biXPelsPerMeter
            0x00,0x00,0x00,0x00, //biYPelsPerMeter
            0x00,0x00,0x00,0x00, //biClrUsed
            0x00,0x00,0x00,0x00, //biClrImportant
            //END BITMAPINFOHEADER

            //BITMAP DATA (4 bytes per pixel)
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF ,0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF,
            0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF,
            0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF,
            0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF,
            0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF,
            0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF,
            0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0x00,0xFF,0xFF, 0x00,0xFF,0x00,0xFF,

            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF,
            0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF, 0x00,0xFF,0x00,0xFF
      };
      *pOut = sizeof(icon_data);
      return icon_data;
};

int http_headers::content_length()
{
  if(headers.find("content-length") != headers.end())
  {
     std::stringstream ssLength(headers.find("content-length")->second);
     int content_length;
     ssLength >> content_length;
     return content_length;
  }
  return 0;
};

void http_headers::on_read_header(std::string line)
{

  std::stringstream ssHeader(line);
  std::string headerName;
  std::getline(ssHeader, headerName, ':');

  std::string value;
  std::getline(ssHeader, value);
  headers[headerName] = value;
};

void http_headers::on_read_request_line(std::string line)
{
  std::stringstream ssRequestLine(line);
  ssRequestLine >> method;
  ssRequestLine >> url;
  ssRequestLine >> version;
   std::cout << "request for resource: " << url << std::endl;
};

void session::read_body(std::shared_ptr<session> pThis)
{
  int nbuffer = 1000;
  std::shared_ptr<std::vector<char>> bufptr = std::make_shared<std::vector<char>>(nbuffer);
  asio::async_read(pThis->socket, boost::asio::buffer(*bufptr, nbuffer), [pThis](const boost::system::error_code& e, std::size_t s)
  {
  });
};

void session::read_next_line(std::shared_ptr<session> pThis, HandleKits &kits)
{
  asio::async_read_until(pThis->socket, pThis->buff, '\r', [pThis, &kits](const boost::system::error_code& e, std::size_t s)
  {
     std::string line, ignore;
     std::istream stream {&pThis->buff};
     std::getline(stream, line, '\r');
     std::getline(stream, ignore, '\n');
     pThis->headers.on_read_header(line);

     if(line.length() == 0)
     {
        if(pThis->headers.content_length() == 0)
        {
           std::shared_ptr<std::string> str = std::make_shared<std::string>(pThis->headers.get_response(kits));
           asio::async_write(pThis->socket, boost::asio::buffer(str->c_str(), str->length()), [pThis, str](const boost::system::error_code& e, std::size_t s)
           {
              //std::cout << "done" << std::endl;
           });
        }
        else
        {
           pThis->read_body(pThis);
        }
     }
     else
     {
        pThis->read_next_line(pThis, kits);
     }
  });
};

void session::read_first_line(std::shared_ptr<session> pThis, HandleKits &kits)
{
  asio::async_read_until(pThis->socket, pThis->buff, '\r', [pThis, &kits](const boost::system::error_code& e, std::size_t s)
  {
     std::string line, ignore;
     std::istream stream {&pThis->buff};
     std::getline(stream, line, '\r');
     std::getline(stream, ignore, '\n');
     pThis->headers.on_read_request_line(line);
     pThis->read_next_line(pThis, kits);
  });
};

void session::interact(std::shared_ptr<session> pThis, HandleKits &kits)
{
  read_first_line(pThis, kits);
};


HandleKits::HandleKits():
    kitsRunning(false)
    {
        kits.setupOptions();
    }

void HandleKits::runKits()
{
    if (kitsRunning) {
        kits.join();
        kitsRunning = false;
    }
    kits._forked = false;
    int argc=9;
    char* argv[9]={"zapps", "kits", "-b", "tpcb", "--duration", "90", "-t", "1", "--load"};
    po::store(po::parse_command_line(argc,argv,kits.getOptions()), vm);
    po::notify(vm);
    kits.setOptionValues(vm);

    kits.fork();
    kitsRunning = true;
    //kits.shoreEnv->gatherstats_sm();

};
string HandleKits::getStats()
{
    std::stringstream ssOut, ssReturn;
    kits.getShoreEnv()->gatherstats_sm(ssOut);
    string varJson, parJson;
    ssReturn << "{";
    while (ssOut >> varJson) {
        ssOut >> parJson;
        ssReturn << "\"" << varJson << "\" : \"" << parJson << "\", ";
    }
    string strReturn = ssReturn.str();
    strReturn[strReturn.length()-1] = '\0';
    strReturn[strReturn.length()-2] = '}';

    return strReturn;
};
