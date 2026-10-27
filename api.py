from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from connection import send_to_event_hub, generate_uber_ride_confirmation

app = FastAPI(
    title="Uber Real-Time Data Streaming API",
    description="Real-time event producer streaming Uber ride data into Azure Event Hub",
    version="1.0.0"
)

templates = Jinja2Templates(directory="templates")


@app.get("/health")
def health_check():
    """Health check endpoint for monitoring."""
    return {"status": "healthy", "service": "uber-streaming-producer"}


@app.get("/")
def booking_home(request: Request):
    """Renders the ride booking home dashboard."""
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/book")
def book_ride(request: Request):
    """
    Simulates booking a single Uber ride and streams the event to Azure Event Hub.
    """
    ride = generate_uber_ride_confirmation()
    success = send_to_event_hub(ride)
    return templates.TemplateResponse(
        "confirmation.html",
        {
            "request": request,
            "ride": ride,
            "event_sent": success
        }
    )


@app.get("/api/rides/simulate")
def simulate_rides(count: int = Query(default=5, ge=1, le=100)):
    """
    Generates and streams a batch of simulated ride events to Azure Event Hub.
    Useful for streaming load simulation.
    """
    rides = [generate_uber_ride_confirmation() for _ in range(count)]
    success = send_to_event_hub(rides)
    return JSONResponse(
        content={
            "status": "success" if success else "failed",
            "events_generated": len(rides),
            "streamed_to_event_hub": success,
            "sample_ride": rides[0] if rides else None
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
