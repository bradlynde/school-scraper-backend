import { NextRequest, NextResponse } from 'next/server';

/**
 * Test endpoint to debug Instacart API connection
 */
export async function GET(request: NextRequest) {
  const INSTACART_API_KEY = process.env.INSTACART_API_KEY;
  
  if (!INSTACART_API_KEY) {
    return NextResponse.json({ error: 'API key not configured' }, { status: 400 });
  }

  // Test with minimal request - only name is required per OpenAPI spec
  const testItems = [
    { 
      name: 'milk' // Only required field
    }
  ];

  // Use correct base URL from OpenAPI spec
  const apiBaseUrl = 'https://connect.dev.instacart.tools';
  const endpoint = '/idp/v1/products/products_link';
  
  // Use API key as provided (with "keys." prefix)
  const apiKey = INSTACART_API_KEY.trim();

  const headers: HeadersInit = {
    'Authorization': `Bearer ${apiKey}`,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  };

  const requestBody = {
    title: 'Test Shopping List',
    line_items: testItems,
  };

  try {
    console.log('Testing Instacart API:', {
      endpoint: `${apiBaseUrl}${endpoint}`,
      authHeaderPrefix: `Bearer ${apiKey.substring(0, 15)}...`,
      requestBody,
    });

    const response = await fetch(`${apiBaseUrl}${endpoint}`, {
      method: 'POST',
      headers,
      body: JSON.stringify(requestBody),
    });

    const responseText = await response.text();
    let responseData;
    try {
      responseData = JSON.parse(responseText);
    } catch {
      responseData = responseText;
    }

    return NextResponse.json({
      status: response.status,
      statusText: response.statusText,
      headers: Object.fromEntries(response.headers.entries()),
      response: responseData,
      endpoint: `${apiBaseUrl}${endpoint}`,
      requestBody,
      requestHeaders: {
        Authorization: `Bearer ${apiKey.substring(0, 20)}...`,
        'Content-Type': headers['Content-Type'],
        'Accept': headers['Accept'],
      },
    });
  } catch (error) {
    return NextResponse.json({
      error: error instanceof Error ? error.message : String(error),
      endpoint: `${apiBaseUrl}${endpoint}`,
    }, { status: 500 });
  }
}

