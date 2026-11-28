package com.recetas.service;

import com.recetas.dto.DetalleRecetaRequestDTO;
import com.recetas.dto.RecetaRequestDTO;
import com.recetas.dto.ReservaStockRequestDTO;
import com.recetas.entity.DetalleRecetaEntity;
import com.recetas.entity.EstadoReceta;
import com.recetas.entity.RecetaEntity;
import com.recetas.repository.RecetaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
public class RecetaService {

    private static final Logger log = LoggerFactory.getLogger(RecetaService.class);

    private final RecetaRepository recetaRepository;
    private final ReservaStockPublisher reservaStockPublisher;
    private final String urlColaSqs;

    public RecetaService(
            RecetaRepository recetaRepository,
            ReservaStockPublisher reservaStockPublisher,
            @Value("${aws.sqs.reserva-url:}") String urlColaSqs) {

        this.recetaRepository = recetaRepository;
        this.reservaStockPublisher = reservaStockPublisher;
        this.urlColaSqs = urlColaSqs;
    }

    private boolean transicionValida(
            EstadoReceta estadoActual,
            EstadoReceta nuevoEstado) {

        return switch (estadoActual) {
            case PENDIENTE_RESERVA ->
                    nuevoEstado == EstadoReceta.STOCK_RESERVADO
                            || nuevoEstado == EstadoReceta.RESERVA_RECHAZADA;
            case STOCK_RESERVADO ->
                    nuevoEstado == EstadoReceta.DISPENSADA;
            case RESERVA_RECHAZADA, DISPENSADA -> false;
        };
    }

    @Transactional
    public RecetaEntity crearReceta(RecetaRequestDTO request) {

        RecetaEntity receta = new RecetaEntity();
        receta.setIdMedico(request.getIdMedico());
        receta.setPacienteNombre(request.getPacienteNombre());
        receta.setIdSucursal(request.getIdSucursal());
        receta.setFechaEmision(LocalDateTime.now());
        receta.setEstado(EstadoReceta.PENDIENTE_RESERVA);

        List<DetalleRecetaEntity> detalles = new ArrayList<>();

        for (DetalleRecetaRequestDTO detalleRequest : request.getDetalles()) {
            // No permitir cantidades de medicamentos iguales o menores a cero.
            if (detalleRequest.getCantidad() == null || detalleRequest.getCantidad() <= 0) {
                throw new IllegalArgumentException("La cantidad debe ser mayor a 0");
            }

            DetalleRecetaEntity detalle = new DetalleRecetaEntity();
            detalle.setReceta(receta);
            detalle.setIdMedicamento(detalleRequest.getIdMedicamento());
            detalle.setCantidad(detalleRequest.getCantidad());
            detalles.add(detalle);
        }

        receta.setDetalles(detalles);

        // Guarda la receta y obtiene el ID que se usará en el mensaje SQS.
        RecetaEntity guardada = recetaRepository.save(receta);

        // Mientras no exista una cola configurada, conservamos el flujo local.
        if (urlColaSqs != null && !urlColaSqs.isBlank()) {
            ReservaStockRequestDTO solicitud = new ReservaStockRequestDTO();
            solicitud.setIdReceta(guardada.getId());
            solicitud.setIdSucursal(guardada.getIdSucursal());
            solicitud.setMedicamentos(new ArrayList<>(request.getDetalles()));

            // Solo publica después de confirmar la transacción de MySQL.
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            try {
                                reservaStockPublisher.publicarReserva(solicitud);
                                log.info("Solicitud de reserva enviada a SQS para receta {}", guardada.getId());
                            } catch (RuntimeException e) {
                                // La receta permanece guardada como PENDIENTE_RESERVA.
                                log.error("No se pudo publicar en SQS la receta {}", guardada.getId(), e);
                            }
                        }
                    }
            );
        }

        return guardada;
    }

    public List<RecetaEntity> listarRecetas() {
        return recetaRepository.findAll();
    }

    public RecetaEntity buscarPorId(Integer id) {
        return recetaRepository.findById(id).orElse(null);
    }

    public List<RecetaEntity> listarPorEstado(EstadoReceta estado) {
        return recetaRepository.findByEstado(estado);
    }

    public RecetaEntity actualizarEstado(Integer id, EstadoReceta nuevoEstado) {

        RecetaEntity receta = recetaRepository.findById(id).orElse(null);

        if (receta == null) {
            return null;
        }

        EstadoReceta estadoActual = receta.getEstado();

        if (!transicionValida(estadoActual, nuevoEstado)) {
            throw new IllegalStateException(
                    "No se puede cambiar el estado de "
                            + estadoActual + " a " + nuevoEstado
            );
        }

        receta.setEstado(nuevoEstado);
        return recetaRepository.save(receta);
    }

    public boolean eliminarReceta(Integer id) {

        if (!recetaRepository.existsById(id)) {
            return false;
        }

        recetaRepository.deleteById(id);
        return true;
    }

}