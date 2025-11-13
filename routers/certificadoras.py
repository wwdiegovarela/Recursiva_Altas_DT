from fastapi import APIRouter

# Reexpone todos los routers existentes bajo /certificadoras
from routers import walmart, telefonica, santotomas, indumotora, unimarc, seniorsuites, resultados

router = APIRouter(prefix="/certificadoras", tags=["Certificadoras"])

# Monta los routers existentes conservando sus prefijos originales
router.include_router(walmart.router)
router.include_router(telefonica.router)
router.include_router(santotomas.router)
router.include_router(indumotora.router)
router.include_router(unimarc.router)
router.include_router(seniorsuites.router)
router.include_router(resultados.router)


