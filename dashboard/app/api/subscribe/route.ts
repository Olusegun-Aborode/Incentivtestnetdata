import { NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';

export async function POST(request: Request) {
  try {
    const { email } = await request.json();

    if (!email) {
      return NextResponse.json({ error: 'Email is required' }, { status: 400 });
    }

    const publicationId = process.env.BEEHIIV_PUBLICATION_ID;
    const apiKey = process.env.BEEHIIV_API_KEY;

    if (!publicationId || !apiKey) {
      // If Beehiiv isn't configured, still allow access (dev mode)
      console.warn('BEEHIIV_PUBLICATION_ID or BEEHIIV_API_KEY not set — allowing access without subscription');
      return NextResponse.json({ status: 'ok', dev: true });
    }

    const response = await fetch(
      `https://api.beehiiv.com/v2/publications/${publicationId}/subscriptions`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          email,
          reactivate_existing: true,
          utm_source: 'incentiv-dashboard',
        }),
      }
    );

    if (!response.ok) {
      const errorData = await response.json();
      return NextResponse.json(
        { error: 'Failed to subscribe', details: errorData },
        { status: response.status }
      );
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Failed to subscribe:', error);
    return NextResponse.json({ error: 'Failed to subscribe' }, { status: 500 });
  }
}
